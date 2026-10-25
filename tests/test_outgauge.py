import socket
import os
from pathlib import Path
import select
from struct import pack_into
from threading import Condition
from time import monotonic
import unittest
from unittest.mock import patch

from boxflat.connection_manager import MozaConnectionManager, MozaSerialDevice
from boxflat.moza_command import MozaCommand, MOZA_COMMAND_WRITE
from boxflat.outgauge import OutGaugeListener, OutGaugePacket, RpmScale
from boxflat.serial_handler import SerialHandler


SERIAL_DATA = Path(__file__).resolve().parents[1] / "data" / "serial.yml"


def packet_bytes(rpm, size=96, car=b"beam"):
    data = bytearray(size)
    data[4:8] = car
    pack_into("<f", data, 16, rpm)
    return bytes(data)


class RecordingSerialHandler:
    def __init__(self):
        self.messages = []
        self.changed = Condition()
        self.stopped = False

    def write_bytes(self, message):
        with self.changed:
            self.messages.append(message)
            self.changed.notify_all()

    def wait_for(self, predicate, timeout=2):
        with self.changed:
            return self.changed.wait_for(lambda: predicate(self.messages), timeout)

    def stop(self):
        self.stopped = True

    def flush(self):
        return True


class PacketTests(unittest.TestCase):
    def test_classic_and_beamng_packets_use_little_endian_rpm_at_offset_16(self):
        for size in (92, 96):
            with self.subTest(size=size):
                data = bytearray(packet_bytes(4321.5, size))
                pack_into("<f", data, 12, 87)  # speed, not RPM
                packet = OutGaugePacket.from_bytes(data)
                self.assertEqual(packet.car, b"beam")
                self.assertEqual(packet.rpm, 4321.5)

    def test_short_and_invalid_rpm_packets_are_ignored(self):
        self.assertIsNone(OutGaugePacket.from_bytes(bytes(91)))
        for rpm in (float("nan"), float("inf"), float("-inf"), -1):
            with self.subTest(rpm=rpm):
                self.assertIsNone(OutGaugePacket.from_bytes(packet_bytes(rpm)))

    def test_scale_tracks_redline_resets_for_new_car_and_handles_engine_off(self):
        scale = RpmScale()
        for rpm, expected in ((0, 0), (800, 0), (3900, 0x1f), (7000, 0x3ff)):
            self.assertEqual(scale.bitmask(OutGaugePacket(b"beam", rpm)), expected)
        self.assertEqual(scale.bitmask(OutGaugePacket(b"beam", 10000)), 0x3ff)
        self.assertEqual(scale.bitmask(OutGaugePacket(b"beam", 5400)), 0x1f)
        self.assertEqual(scale.bitmask(OutGaugePacket(b"LFS\0", 3900)), 0x1f)


class ConnectionFixture(unittest.TestCase):
    def setUp(self):
        self.cm = MozaConnectionManager(str(SERIAL_DATA))
        self.handler = RecordingSerialHandler()
        self.base = MozaSerialDevice("base", "usb-Gudsen_MOZA_R5_Base_test", self.handler)
        self.cm._serial_devices["base"] = self.base

    def tearDown(self):
        self.cm.shutdown()


class SharedConnectionTests(ConnectionFixture):
    def test_legacy_frames_match_moza_rev_and_share_settings_writer(self):
        with patch("boxflat.connection_manager.SerialHandler") as new_handler:
            self.assertTrue(self.cm.send_rpm_telemetry(0x3ff))
            self.assertEqual(self.handler.messages, [
                bytes.fromhex("7e 02 40 13 04 01 e5"),
                bytes.fromhex("7e 06 41 13 fd de 00 00 03 ff c2"),
            ])
            self.cm.set_setting(1, "base-ffb-disable")
            self.assertEqual(len(self.handler.messages), 3)
            self.assertTrue(self.cm.send_rpm_telemetry(0))
            self.assertEqual(self.handler.messages[-1],
                bytes.fromhex("7e 06 41 13 fd de 00 00 00 00 c0"))
            self.assertEqual(len(self.handler.messages), 4)  # setup only once
            new_handler.assert_not_called()

    def test_modern_wheel_uses_little_endian_mask_and_current_wheel_id(self):
        self.base.path = "usb-Gudsen_MOZA_R9_Base_test"
        self.cm.send_rpm_telemetry(0x3ff)
        self.assertEqual(self.handler.messages[-1],
            bytes.fromhex("7e 04 3f 17 1a 00 ff 03 01"))
        self.cm._serial_data["device-ids"]["wheel"] = 21
        self.cm.send_rpm_telemetry(0)
        self.assertEqual(self.handler.messages[-1][3], 21)
        self.assertEqual(len(self.handler.messages), 4)  # new target gets setup

    def test_r3_uses_legacy_and_reconnect_reinitializes_indicator_mode(self):
        self.base.path = "usb-Gudsen_MOZA_R3_Base_test"
        self.cm.send_rpm_telemetry(0x3ff)
        replacement = RecordingSerialHandler()
        self.base.serial_handler = replacement
        self.cm.send_rpm_telemetry(0x3ff)
        self.assertEqual(replacement.messages, self.handler.messages)

    def test_absent_base_dry_run_shutdown_and_exclusive_settings_skip_writes(self):
        self.cm._dry_run = True
        self.assertFalse(self.cm.send_rpm_telemetry(1))
        self.cm._dry_run = False
        self.cm._exclusive_access.clear()
        self.assertFalse(self.cm.send_rpm_telemetry(1))
        self.cm._exclusive_access.set()
        self.cm._serial_devices.clear()
        self.assertFalse(self.cm.send_rpm_telemetry(1))
        self.cm._serial_devices["base"] = self.base
        self.cm.shutdown()
        self.assertFalse(self.cm.send_rpm_telemetry(1))
        self.assertEqual(self.handler.messages, [])

    def test_legacy_telemetry_is_not_forwarded_to_hub(self):
        hub = RecordingSerialHandler()
        self.cm._serial_devices["hub"] = MozaSerialDevice("hub", "hub", hub)
        self.cm.send_rpm_telemetry(0x3ff)
        self.assertEqual(hub.messages, [])

    def test_telemetry_escapes_body_before_checksum_and_escapes_checksum(self):
        command = MozaCommand()
        command.set_data_from_name("send-rpm-telemetry", self.cm.get_command_data(), "base")
        command.device_id = 19
        command.set_payload(0x7e)
        self.assertEqual(command.prepare_message(126, MOZA_COMMAND_WRITE, 13, escape=True),
            bytes.fromhex("7e 06 41 13 fd de 00 00 00 7e 7e bc"))
        # This mask produces a checksum of 0x7e in the legacy frame.
        self.cm.send_rpm_telemetry(0x1bd)
        self.assertEqual(self.handler.messages[-1],
            bytes.fromhex("7e 06 41 13 fd de 00 00 01 bd 7e 7e"))


class UdpListenerTests(ConnectionFixture):
    def setUp(self):
        super().setUp()
        self.listener = OutGaugeListener(self.cm, port=0)
        self.sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def tearDown(self):
        self.listener.stop()
        self.sender.close()
        super().tearDown()

    def send(self, rpm, size=96):
        self.sender.sendto(packet_bytes(rpm, size), ("127.0.0.1", self.listener.port))

    def test_udp_drives_shared_writer_clears_off_and_on_stop(self):
        self.assertTrue(self.listener.set_enabled(True))
        self.send(7000, size=92)
        full = bytes.fromhex("7e 06 41 13 fd de 00 00 03 ff c2")
        clear = bytes.fromhex("7e 06 41 13 fd de 00 00 00 00 c0")
        self.assertTrue(self.handler.wait_for(lambda messages: full in messages))
        self.send(0)
        self.assertTrue(self.handler.wait_for(lambda messages: messages[-1] == clear))
        self.send(7000)
        self.assertTrue(self.handler.wait_for(lambda messages: messages[-1] == full))
        self.listener.stop()
        self.assertEqual(self.handler.messages[-1], clear)
        self.assertFalse(self.listener.enabled)
        # The UDP socket is released immediately on stop.
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp:
            udp.bind(("127.0.0.1", self.listener.port))

    def test_timeout_clears_leds_and_resets_adaptive_scale(self):
        with patch("boxflat.outgauge.IDLE_TIMEOUT", 0.1):
            self.listener.set_enabled(True)
            self.send(12000)
            self.assertTrue(self.handler.wait_for(lambda messages: len(messages) >= 3))
            self.assertEqual(self.handler.messages[-1],
                bytes.fromhex("7e 06 41 13 fd de 00 00 00 00 c0"))
            self.send(7000)
            self.assertTrue(self.handler.wait_for(lambda messages: messages[-1][6:10] == b"\0\0\x03\xff"))

    def test_busy_port_reports_failure_and_can_be_retried(self):
        self.listener.set_enabled(True)
        other = OutGaugeListener(self.cm, port=self.listener.port)
        try:
            with self.assertLogs("boxflat.outgauge", level="WARNING"):
                self.assertFalse(other.set_enabled(True))
            self.assertIn("Cannot listen", other.status)
            self.assertFalse(other.enabled)
            self.listener.stop()
            self.assertTrue(other.set_enabled(True))
        finally:
            other.stop()

    def test_idle_start_and_dry_run_do_not_touch_hardware(self):
        dry_run = OutGaugeListener(self.cm, dry_run=True)
        self.assertFalse(dry_run.set_enabled(True))
        self.listener.set_enabled(True)
        self.sender.sendto(b"invalid", ("127.0.0.1", self.listener.port))
        self.listener.stop()
        self.assertEqual(self.handler.messages, [])

    def test_burst_is_rate_limited_and_keeps_latest_value(self):
        self.listener.set_enabled(True)
        started = monotonic()
        for i in range(1000):
            self.send(1000 if i % 2 else 7000)
        self.send(7000)
        full = bytes.fromhex("7e 06 41 13 fd de 00 00 03 ff c2")
        self.assertTrue(self.handler.wait_for(lambda messages: messages[-1] == full))
        elapsed = monotonic() - started
        self.assertLessEqual(len(self.handler.messages), 2 + int(elapsed * 30))

    def test_port_change_restarts_listener_and_releases_previous_port(self):
        self.listener.set_enabled(True)
        previous = self.listener.port
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as available:
            available.bind(("127.0.0.1", 0))
            new_port = available.getsockname()[1]
        self.assertTrue(self.listener.set_port(new_port))
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp:
            udp.bind(("127.0.0.1", previous))
        self.send(7000)
        self.assertTrue(self.handler.wait_for(lambda messages: len(messages) >= 2))

    def test_socket_creation_failure_is_reported(self):
        with patch("boxflat.outgauge.socket.socket", side_effect=OSError("no sockets")):
            with self.assertLogs("boxflat.outgauge", level="WARNING"):
                self.assertFalse(self.listener.set_enabled(True))
        self.assertIn("no sockets", self.listener.status)


class SerialDrainTests(unittest.TestCase):
    def test_queued_led_clear_reaches_serial_before_shutdown(self):
        cm = MozaConnectionManager(str(SERIAL_DATA))
        listener = OutGaugeListener(cm, port=0)
        self.assertTrue(listener.set_enabled(True))
        master, slave = os.openpty()
        handler = SerialHandler(os.ttyname(slave), 126, "test-base")
        try:
            expected = bytes.fromhex("7e 06 41 13 fd de 00 00 03 ff c2")
            expected += bytes.fromhex("7e 06 41 13 fd de 00 00 00 00 c0")
            handler.write_bytes(expected[:11])
            handler.write_bytes(expected[11:])
            self.assertTrue(handler.flush(timeout=2))
            listener.stop()
            # The live serial child must not retain the UDP descriptor it
            # inherited through fork when device discovery started it.
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp:
                udp.bind(("127.0.0.1", listener.port))
            handler.stop()
            received = b""
            deadline = monotonic() + 2
            while len(received) < len(expected) and monotonic() < deadline:
                if select.select([master], [], [], 0.1)[0]:
                    received += os.read(master, 4096)
            self.assertEqual(received, expected)
        finally:
            listener.stop()
            cm.shutdown()
            handler.stop()
            os.close(master)
            os.close(slave)


if __name__ == "__main__":
    unittest.main()
