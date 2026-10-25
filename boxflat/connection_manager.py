# Copyright (c) 2025, Tomasz Pakuła Using Arch BTW

import yaml
import os.path
from .moza_command import *
from serial import Serial
from threading import Thread, Lock, Event
import time
from .hid_handler import MozaHidDevice
from .subscription import SubscriptionList, EventDispatcher, BlockingValue
from queue import SimpleQueue
from .serial_handler import SerialHandler
import re
from dataclasses import replace
from .outgauge import RpmSettings, HEARTBEAT_INTERVAL

CM_RETRY_COUNT=1

HidDeviceMapping = {
    "base"       : MozaHidDevice.BASE,
    "handbrake"  : MozaHidDevice.HANDBRAKE,
    "hpattern"   : MozaHidDevice.HPATTERN,
    "sequential" : MozaHidDevice.SEQUENTIAL,
    "pedals"     : MozaHidDevice.PEDALS,
    "hub"        : MozaHidDevice.HUB,
    "stalks"     : MozaHidDevice.STALKS,
}

SerialDeviceMapping = {
    MozaHidDevice.BASE       : "base",
    MozaHidDevice.HANDBRAKE  : "handbrake",
    MozaHidDevice.HPATTERN   : "hpattern",
    MozaHidDevice.SEQUENTIAL : "sequential",
    MozaHidDevice.PEDALS     : "pedals",
    MozaHidDevice.HUB        : "hub",
    "gudsen_e-stop"          : "estop",
    MozaHidDevice.STALKS     : "stalks",
    "moza_.*_digital_dash"   : "dash",
}


class MozaSerialDevice():
    name: str
    path: str
    serial_handler: SerialHandler

    def __init__(self, name="", path="", handler=None):
        self.name = name
        self.path = path
        self.serial_handler = handler



class MozaConnectionManager(EventDispatcher):
    def __init__(self, serial_data_path: str, dry_run=False):
        super().__init__()

        self._serial_data = None
        self._dry_run = dry_run
        self._shutdown = Event()

        self._cycling = False
        self._old_cycling = False

        self._serial_devices: dict[str, MozaSerialDevice] = {}
        self._devices_lock = Lock()
        self._rpm_lock = Lock()
        self._rpm_target = None
        self._rpm_settings = RpmSettings()
        self._rpm_protocol = None
        self._rpm_colors = [None] * 10
        self._rpm_color_target = None
        self._rpm_colors_sent_at = 0.0
        self._exclusive_access = Event()
        self._exclusive_access.set()

        with open(serial_data_path) as stream:
            try:
                self._serial_data = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
                self._shutdown.set()
                quit(1)

        self._device_ids: dict[str, int] = self._serial_data["device-ids"]

        # register events
        self._command_list: list[str] = []
        self._polling_list: list[str] = []
        for device in self._serial_data["commands"]:
            if self._device_ids[device] == -1:
                continue

            for command, data in self._serial_data["commands"][device].items():
                event_name = f"{device}-{command}"
                self._command_list.append(event_name)

                if data["read"] == -1:
                    continue

                self._polling_list.append(event_name)

        self._register_events(*self._polling_list)
        self._register_events("device-connected", "hid-device-connected")
        self._register_events("device-disconnected", "hid-device-disconnected")

        # Keep the same values used by the UI, including device readback at
        # startup and presets applied while the window is in the background.
        self._rpm_setting_names = ("wheel-rpm-mode", "wheel-rpm-timings", "wheel-telemetry-mode",
                                  *(f"wheel-rpm-value{i+1}" for i in range(10)),
                                  *(f"wheel-rpm-color{i+1}" for i in range(10)))
        for name in self._rpm_setting_names:
            self.subscribe(name, self._cache_rpm_setting, name)

        self._serial_lock = Lock()
        self._refresh_cont = Event()

        self._connected_subscriptions: dict[str, SubscriptionList] = {}
        self._connected_thread = None
        self._connected_lock = Lock()
        self.subscribe_connected("wheel-telemetry-mode", self._rpm_modern_connected)

        self._message_start= int(self._serial_data["message-start"])
        self._magic_value = int(self._serial_data["magic-value"])
        self._serial_path = "/dev/serial/by-id"
        # self._serial_path = "/dev/pts"


    def shutdown(self, *rest):
        self._dispatch("shutdown")
        self._shutdown.set()
        self.refresh_cont(False)
        with self._devices_lock:
            devices = list(self._serial_devices.values())
        for device in devices:
            device.serial_handler.stop()


    def device_discovery(self, *args):
        # print("\nDevice discovery...")
        if not os.path.exists(self._serial_path):
            # print("No devices found!")
            self._handle_devices({})
            return

        devices = []
        for device in os.listdir(self._serial_path):
            if device.lower().find("gudsen"):
                devices.append(os.path.join(self._serial_path, device))

        serial_devices: dict[str, MozaSerialDevice] = {}
        for path in devices:
            for part, name in SerialDeviceMapping.items():
                part = part.replace(" ", "_")
                if not re.search(part, path.lower()):
                    continue

                serial_devices[name] = MozaSerialDevice(name, path)
                # print(f"\"{name}\" found".title())

        self._handle_devices(serial_devices)
        # print("Device discovery end\n")


    def _handle_devices(self, new_devices: dict):
        old_devices = None
        with self._devices_lock:
            old_devices = self._serial_devices

        if ("base" in new_devices) != ("base" in old_devices):
            with self._rpm_lock:
                self._rpm_settings = RpmSettings()
                self._rpm_protocol = None
                self._rpm_colors = [None] * 10
                self._rpm_color_target = None

        for name in new_devices:
            if name in old_devices:
                new_devices[name].serial_handler = old_devices[name].serial_handler
                continue

            new_devices[name].serial_handler = SerialHandler(
                new_devices[name].path,
                self._message_start, name)

            new_devices[name].serial_handler.subscribe(self._receive_data, name)
            self._dispatch("device-connected", name)
            if name in HidDeviceMapping:
                self._dispatch("hid-device-connected", HidDeviceMapping[name])

        for name, device in old_devices.items():
            if name in new_devices:
                continue

            device.serial_handler.stop()
            self._dispatch("device-disconnected", name)
            if name in HidDeviceMapping:
                self._dispatch("hid-device-disconnected", HidDeviceMapping[name])


        with self._devices_lock:
            self._serial_devices = new_devices

        if len(new_devices) == 0 and self._refresh_cont.is_set():
            self.refresh_cont(False)

        elif len(new_devices) > 0 and not self._refresh_cont.is_set():
            self.refresh_cont(True)


    def subscribe_connected(self, command: str, callback, *args):
        if not command in self._connected_subscriptions:
            self._connected_subscriptions[command] = SubscriptionList()
        self._connected_subscriptions[command].append(callback, *args)


    def refresh_cont(self, active: bool):
        if active:
            self._refresh_cont.set()
            Thread(daemon=True, target=self._polling_thread).start()
        else:
            self._refresh_cont.clear()


    def _polling_thread(self):
        while self._refresh_cont.is_set():
            time.sleep(2)

            for command in self._polling_list:
                if command.startswith("estop-receive"):
                    continue

                if self._event_sub_count(command) == 0:
                    continue

                # print("Polling data: " + command)
                self._get_setting(command)
                time.sleep(0.002)


    def _device_polling(self):
        time.sleep(1)
        while not self._shutdown.is_set():
            self.device_discovery()

            with self._connected_lock:
                lists = self._connected_subscriptions.copy()

            for command, subs in lists.items():
                value = self.get_setting(command)
                if value is None:
                    value = self.get_setting(command)
                if value is None:
                    value = -1
                subs.call(value)

            time.sleep(3)
        self._connected_thread = None


    def set_cont_active(self, active: bool):
        if active:
            self._cont_active.set()
        else:
            self._cont_active.clear()


    def set_write_active(self, *args):
        if not self._connected_thread:
            self._connected_thread = Thread(daemon=True, target=self._device_polling)
            self._connected_thread.start()


    def get_device_id(self, device_type: str) -> int:
        id = int(self._serial_data["device-ids"][device_type])
        if device_type != "base" and device_type in self._serial_devices:
            id = int(self._serial_data["device-ids"]["main"])
        return id


    def _get_device_handler(self, device_type: str) -> SerialHandler:
        device_handler = None
        with self._devices_lock:
            if device_type in self._serial_devices:
                device_handler = self._serial_devices[device_type].serial_handler

            elif "base" in self._serial_devices:
                # print(f"Redirect {device_type} to base")
                device_handler = self._serial_devices["base"].serial_handler

        return device_handler


    def _get_hub_handler(self) -> SerialHandler:
        if "hub" not in self._serial_devices:
            return
        # print(f"Redirect {device_type} to hub")
        return self._serial_devices["hub"].serial_handler


    def send_rpm_telemetry(self, bitmask: int) -> bool:
        """Queue a 10-LED RPM bar on the existing wheelbase connection.

        Modern telemetry capability takes precedence over the base model.
        R3/R5 ES wheels require the base's legacy telemetry command. Do
        not route that command to the wheel ID: its firmware can lock up.
        Wire format and model detection follow moza-rev/src/moza.rs.
        """
        if self._dry_run or self._shutdown.is_set() or not self._exclusive_access.is_set():
            return False

        with self._devices_lock:
            base = self._serial_devices.get("base")
        if base is None:
            return False

        with self._rpm_lock:
            protocol = self._rpm_protocol
        legacy = protocol == "legacy" or (protocol is None and
            bool(re.search(r"_r[35]_", base.path.lower())))
        device = "base" if legacy else "wheel"
        device_id = self.get_device_id(device)
        target = (base.serial_handler, device_id, legacy)
        bitmask = max(0, min(int(bitmask), 0x3ff))

        def queue_command(name, value):
            command = MozaCommand()
            command.set_data_from_name(name, self._serial_data["commands"], device)
            command.device_id = device_id
            command.set_payload(value)
            message = command.prepare_message(
                self._message_start, MOZA_COMMAND_WRITE, self._magic_value, escape=True)
            base.serial_handler.write_bytes(message)

        with self._rpm_lock:
            if target != self._rpm_target:
                queue_command("es-rpm-indicator-mode" if legacy else "telemetry-mode", 1)
                self._rpm_target = target

            # Modern firmware uses a separate palette for telemetry. Copy
            # the colors shown by the Wheel panel, as its telemetry test does.
            colors = tuple(self._rpm_colors)
            color_target = (target, colors)
            now = time.monotonic()
            if not legacy and all(color is not None for color in colors):
                if (self._rpm_color_target != color_target or
                        now - self._rpm_colors_sent_at >= HEARTBEAT_INTERVAL):
                    payload = [value for index, color in enumerate(colors)
                               for value in (index, *color)]
                    queue_command("telemetry-rpm-colors", payload[:20])
                    queue_command("telemetry-rpm-colors", payload[20:])
                    self._rpm_color_target = color_target
                    self._rpm_colors_sent_at = now

            # The window claims the entire bar, including currently dark
            # LEDs. A bare active mask can leave stale firmware LED state.
            # See giantorth/AZOM docs/protocol/leds/color-commands.md.
            queue_command("send-rpm-telemetry" if legacy else "send-rpm-windowed-telemetry",
                          bitmask if legacy else list(
                              bitmask.to_bytes(4, "little") + (0x3ff).to_bytes(4, "little")))
        return True


    def reset_rpm_telemetry(self):
        with self._rpm_lock:
            self._rpm_target = None
            self._rpm_color_target = None


    def get_rpm_settings(self) -> RpmSettings:
        with self._rpm_lock:
            if self._rpm_protocol == "modern":
                # The modern Wheel panel has no hardware timing table. Use
                # a Normal bar and learn the observed peak, including truck
                # engines whose maximum RPM is far below 7000.
                return RpmSettings(mode=0, percent=(75, 79, 82, 85, 87, 88, 89, 90, 92, 94),
                                   redline_floor=0)
            return self._rpm_settings


    def _rpm_modern_connected(self, value):
        with self._rpm_lock:
            if value in (0, 1, 2):
                self._rpm_protocol = "modern"
            elif self._rpm_protocol == "modern":
                self._rpm_protocol = None
                self._rpm_settings = RpmSettings()
                self._rpm_colors = [None] * 10
                self._rpm_color_target = None


    def _cache_rpm_setting(self, value, name, written=False):
        with self._rpm_lock:
            if name == "wheel-telemetry-mode" and value in (0, 1, 2):
                self._rpm_protocol = "modern"
            elif name.startswith("wheel-rpm-color"):
                index = name.removeprefix("wheel-rpm-color")
                if index.isdecimal() and 1 <= int(index) <= 10:
                    if isinstance(value, (list, tuple)) and len(value) == 3:
                        self._rpm_colors[int(index) - 1] = tuple(value)
                        if written:
                            # Static writes repaint the live buffer even if
                            # the RGB is unchanged. Reapply the live palette.
                            self._rpm_color_target = None
            elif name == "wheel-rpm-mode" and value in (0, 1):
                self._rpm_settings = replace(self._rpm_settings, mode=value)
            elif name == "wheel-rpm-timings" and isinstance(value, (list, tuple)):
                if len(value) == 10 and all(isinstance(v, int) and 0 <= v <= 100 for v in value):
                    self._rpm_settings = replace(self._rpm_settings, percent=tuple(value))
            elif name.startswith("wheel-rpm-value"):
                index = name.removeprefix("wheel-rpm-value")
                if index.isdecimal() and 1 <= int(index) <= 10:
                    if isinstance(value, int) and 0 <= value <= 65535:
                        if self._rpm_protocol != "modern":
                            self._rpm_protocol = "legacy"
                        values = list(self._rpm_settings.rpm)
                        values[int(index) - 1] = value
                        self._rpm_settings = replace(self._rpm_settings, rpm=tuple(values))


    def flush_rpm_telemetry(self):
        with self._rpm_lock:
            target = self._rpm_target
        if target is not None:
            target[0].flush()


    def _receive_data(self, data: bytes, device_name: str):
        command, value = MozaCommand.value_from_response(
            data, device_name,
            self._serial_data["commands"],
            self._serial_data["ids-to-names"])

        if value is None or command is None:
            return

        # print(f"{command} received: {data.hex(":")}")
        self._dispatch(command, value)


    def _handle_command_v2(self, command_data: MozaCommand, rw: int) -> bytes:
        message = command_data.prepare_message(self._message_start, rw, self._magic_value)
        device_handler = self._get_device_handler(command_data.device_type)

        if device_handler is not None:
            device_handler.write_bytes(message)

        device_handler = self._get_hub_handler()
        if device_handler is not None:
            device_handler.write_bytes(message)


    def _handle_setting(self, value, command_name: str, device_name: str, rw: int) -> bool:
        command = MozaCommand()
        command.set_data_from_name(command_name, self._serial_data["commands"], device_name)
        command.device_id = self.get_device_id(command.device_type)

        if command.device_id == -1:
            print("Invalid Device ID")
            return

        if rw == MOZA_COMMAND_WRITE and command.write_group == -1:
            print("Command doesn't support WRITE operation: " + command_name)
            return

        elif rw == MOZA_COMMAND_READ and command.read_group == -1:
            print("Command doesn't support READ operation: " + command_name)
            return

        command.set_payload(value)
        # Queue the static setting before publishing it to the live palette.
        # Otherwise the OutGauge thread can send the new palette first and
        # the following static write clobbers it after it was cached as sent.
        self._handle_command_v2(command, rw)
        name = f"{device_name}-{command_name}"
        if rw == MOZA_COMMAND_WRITE and name in self._rpm_setting_names:
            self._cache_rpm_setting(MozaCommand.value_from_data(
                command.payload, command.type, command.payload_length), name, written=True)


    def _split_name(self, command_name: str) -> tuple[str, str]:
        if command_name not in self._command_list:
            print(f"Command not found: {command_name}")
            return "", ""

        device_name = command_name.split("-", maxsplit=1)[0]
        command_name = command_name.split("-", maxsplit=1)[1]
        return command_name, device_name


    def set_setting(self, value, command_name: str, exclusive=False):
        self._exclusive_access.wait()
        if exclusive:
            self._exclusive_access.clear()
            time.sleep(0.005)

        name, device = self._split_name(command_name)
        if name == "":
            return
        self._handle_setting(value, name, device, MOZA_COMMAND_WRITE)

        if exclusive:
            self._exclusive_access.set()

        # if self.get_setting(command_name) != value:
        #     self._handle_setting(value, name, device, MOZA_COMMAND_WRITE)


    def get_setting(self, command_name: str, exclusive=False, custom_value=1):
        self._exclusive_access.wait()
        if exclusive:
            self._exclusive_access.clear()
            time.sleep(0.005)

        value = BlockingValue()

        sub = self.subscribe_once(command_name, value.set_value)
        self._get_setting(command_name, exclusive, custom_value)
        response = value.get_value_no_clear()

        if response is None:
            self._remove_subscription(command_name, sub)

        if exclusive:
            time.sleep(0.01)
            self._exclusive_access.set()

        return response


    def _get_setting(self, command_name: str, exclusive=False, custom_value=1):
        name, device = self._split_name(command_name)
        if name == "":
            return
        self._handle_setting(custom_value, name, device, MOZA_COMMAND_READ)


    def cycle_wheel_id(self, old=False) -> int:
        if old:
            self._old_cycling = True
        else:
            self._cycling = True

        if not (self._old_cycling and self._cycling):
            return 0

        self._old_cycling = False
        self._cycling = False

        with self._devices_lock:
            wid = self._serial_data["device-ids"]["wheel"] - 2

            if wid < self._serial_data["device-ids"]["base"]:
                wid = self._serial_data["device-ids"]["pedals"] - 2

            self._serial_data["device-ids"]["wheel"] = wid

            # print(f"Cycling wheel id. New id: {wid}")
            return wid


    def get_command_data(self) -> dict[str, dict]:
        return self._serial_data["commands"]
