"""OutGauge RPM LEDs, using Boxflat's existing serial writer.

Packet offsets and the adaptive RPM scale are ported from moza-rev's
outgauge.rs, listeners/outgauge.rs and app.rs. No extra device is opened.
"""

from dataclasses import dataclass
import logging
from math import floor, isfinite
from multiprocessing.util import register_after_fork
import socket
from struct import unpack_from
from threading import Event, Thread
from time import monotonic

from boxflat.subscription import SimpleEventDispatcher


DEFAULT_PORT = 4444
INITIAL_REDLINE = 7000
ASSUMED_IDLE = 800
LED_COUNT = 10
MAX_UPDATE_RATE = 30
HEARTBEAT_INTERVAL = 1.0
IDLE_TIMEOUT = 2.0

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OutGaugePacket:
    car: bytes
    rpm: float

    @classmethod
    def from_bytes(cls, data: bytes):
        # Classic LFS packets omit the optional 4-byte ID at offset 92.
        if len(data) < 92:
            return None
        rpm = unpack_from("<f", data, 16)[0]
        if not isfinite(rpm) or rpm < 0:
            return None
        return cls(car=data[4:8], rpm=rpm)


class RpmScale:
    def __init__(self):
        self._car = None
        self._redline = INITIAL_REDLINE

    def bitmask(self, packet: OutGaugePacket) -> int | None:
        if packet.car != self._car:
            self._car = packet.car
            self._redline = INITIAL_REDLINE
        self._redline = max(self._redline, packet.rpm)
        fraction = max(0.0, min(1.0,
            (packet.rpm - ASSUMED_IDLE) / (self._redline - ASSUMED_IDLE)))
        # Rust f32::round rounds positive halves up, unlike Python round.
        lit = floor(fraction * LED_COUNT + 0.5)
        return (1 << lit) - 1


class OutGaugeListener(SimpleEventDispatcher):
    """One daemon worker receives UDP and submits bounded serial updates.

    Subscribers receive a human-readable listener status, including bind
    failures. Starting/stopping is controlled by the application/UI thread.
    """
    def __init__(self, connection_manager, port=DEFAULT_PORT, dry_run=False):
        super().__init__()
        self._cm = connection_manager
        self._port = int(port)
        self._dry_run = dry_run
        self._stop = Event()
        self._thread = None
        self._socket = None
        self.status = "Disabled"
        # SerialHandler forks a process per device. Its child must not keep
        # this socket bound after the application disables/restarts OutGauge.
        register_after_fork(self, OutGaugeListener._close_in_child)

    def _close_in_child(self):
        if self._socket is not None:
            self._socket.close()

    @property
    def port(self):
        return self._port

    @property
    def enabled(self):
        return self._thread is not None and self._thread.is_alive()

    def _set_status(self, status):
        self.status = status
        self._dispatch(status)

    def set_enabled(self, enabled) -> bool:
        if not enabled:
            self.stop()
            return True
        if self.enabled:
            return True
        if self._dry_run:
            self._set_status("OutGauge is disabled in dry-run mode")
            return False
        # Release a socket left behind by a worker that exited on an error.
        if self._thread is not None:
            self.stop()
        udp = None
        try:
            udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp.bind(("127.0.0.1", self._port))
            udp.settimeout(1 / MAX_UPDATE_RATE)
        except (OSError, OverflowError) as error:
            if udp is not None:
                udp.close()
            self._set_status(f"Cannot listen on UDP port {self._port}: {error}")
            logger.warning(self.status)
            return False
        self._socket = udp
        self._port = udp.getsockname()[1]
        self._stop.clear()
        self._cm.reset_rpm_telemetry()
        self._thread = Thread(target=self._run, args=(udp,), daemon=True, name="outgauge")
        self._thread.start()
        self._set_status(f"Listening on 127.0.0.1:{self._port}")
        return True

    def set_port(self, port: int) -> bool:
        port = int(port)
        if not 1 <= port <= 65535:
            raise ValueError("OutGauge port must be between 1 and 65535")
        if port == self._port:
            return True
        restart = self.enabled
        self.stop()
        self._port = port
        return self.set_enabled(True) if restart else True

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
            self._thread = None
        if self._socket is not None:
            self._socket.close()
            self._socket = None
        self._set_status("Disabled")

    def _run(self, udp):
        scale = RpmScale()
        last_packet_at = None
        last_send_at = 0.0
        last_mask = None
        pending_mask = 0
        drove_leds = False
        try:
            while not self._stop.is_set():
                try:
                    data = udp.recv(4096)
                except socket.timeout:
                    data = None
                except OSError as error:
                    self._set_status(f"OutGauge receive error: {error}")
                    break

                now = monotonic()
                # Reset even if a new packet arrived just after the timeout.
                if last_packet_at is not None and now - last_packet_at >= IDLE_TIMEOUT:
                    last_packet_at = None
                    pending_mask = 0
                    scale = RpmScale()

                packet = OutGaugePacket.from_bytes(data) if data is not None else None
                if packet is not None:
                    last_packet_at = now
                    pending_mask = scale.bitmask(packet)

                if last_packet_at is None and not drove_leds:
                    continue
                elapsed = now - last_send_at
                if elapsed < 1 / MAX_UPDATE_RATE:
                    continue
                if pending_mask == last_mask and elapsed < HEARTBEAT_INTERVAL:
                    continue
                if self._cm.send_rpm_telemetry(pending_mask):
                    drove_leds = last_packet_at is not None
                    last_mask = pending_mask
                    last_send_at = now
        finally:
            if drove_leds:
                self._cm.send_rpm_telemetry(0)
            if last_mask is not None:
                self._cm.flush_rpm_telemetry()
            udp.close()
