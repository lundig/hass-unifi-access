"""Unifi Access Hub.

This module interacts with the Unifi Access API server.
"""

import asyncio
from collections.abc import Callable
from datetime import datetime
import json
import logging
import ssl
from threading import Thread
from typing import Any, Literal, TypedDict, cast
import unicodedata
from urllib.parse import urlparse

from requests import request
from requests.exceptions import ConnectionError as ConnError, SSLError
import urllib3
import websocket

from .const import (
    ACCESS_ENTRY_EVENT,
    ACCESS_EXIT_EVENT,
    ACCESS_EVENT,
    DEVICE_NOTIFICATIONS_URL,
    DOOR_LOCK_RULE_URL,
    DOOR_UNLOCK_URL,
    DOORBELL_START_EVENT,
    DOORBELL_STOP_EVENT,
    DOORS_EMERGENCY_URL,
    DOORS_URL,
    STATIC_URL,
    UNIFI_ACCESS_API_PORT,
)
from .door import UnifiAccessDoor
from .errors import ApiAuthError, ApiError

_LOGGER = logging.getLogger(__name__)

EmergencyData = dict[str, bool]


class DoorLockRule(TypedDict):
    """DoorLockRule.

    This class defines the different locking rules.
    """

    type: Literal[
        "keep_lock", "keep_unlock", "custom", "reset", "lock_early", "lock_now"
    ]
    interval: int


class DoorLockRuleStatus(TypedDict):
    """DoorLockRuleStatus.

    This class defines the active locking rule status.
    """

    type: Literal["schedule", "keep_lock", "keep_unlock", "custom", "lock_early", ""]
    ended_time: int


def normalize_door_name(name: str) -> str:
    """Normalize door name for comparison."""
    if not name:
        return ""
    return unicodedata.normalize("NFC", name.strip())


class UnifiAccessHub:
    """UnifiAccessHub.

    This class takes care of interacting with the Unifi Access API.
    """

    def __init__(
        self, host: str, verify_ssl: bool = False, use_polling: bool = False
    ) -> None:
        """Initialize."""
        self.use_polling = use_polling
        self.verify_ssl = verify_ssl
        if self.verify_ssl is False:
            _LOGGER.warning("SSL Verification disabled for %s", host)
            urllib3.disable_warnings()

        host_parts = host.split(":")
        parsed_host = urlparse(host)

        hostname = parsed_host.hostname if parsed_host.hostname else host_parts[0]
        port = (
            parsed_host.port
            if parsed_host.port
            else (host_parts[1] if len(host_parts) > 1 else UNIFI_ACCESS_API_PORT)
        )
        self._api_token = None
        self.host = f"https://{hostname}:{port}"
        self._http_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        self.websocket_host = f"wss://{hostname}:{port}"
        self._websocket_headers = {
            "Upgrade": "websocket",
            "Connection": "Upgrade",
        }
        self._doors: dict[str, UnifiAccessDoor] = {}
        self.evacuation = False
        self.lockdown = False
        self.supports_door_lock_rules = True
        self.update_t = None
        self._callbacks: set[Callable] = set()
        self.loop = asyncio.get_event_loop()

    @property
    def doors(self):
        """Get current doors."""
        return self._doors

    def set_api_token(self, api_token):
        """Set API Access Token."""
        self._api_token = api_token
        self._http_headers["Authorization"] = f"Bearer {self._api_token}"
        self._websocket_headers["Authorization"] = f"Bearer {self._api_token}"

    def update(self):
        """Get latest door data."""
        _LOGGER.debug(
            "Getting door updates from Unifi Access %s Use Polling %s. Doors? %s",
            self.host,
            self.use_polling,
            self.doors.keys(),
        )
        data = self._make_http_request(f"{self.host}{DOORS_URL}")

        for _i, door in enumerate(data):
            if door.get("is_bind_hub") is True:
                door_id = door["id"]
                door_lock_rule = {"type": "", "ended_time": 0}
                if self.supports_door_lock_rules:
                    door_lock_rule = self.get_door_lock_rule(door_id)

                if door_id in self.doors:
                    existing_door = self.doors[door_id]
                    existing_door.name = normalize_door_name(door["name"])
                    existing_door.door_position_status = door.get("door_position_status")
                    existing_door.door_lock_relay_status = door.get(
                        "door_lock_relay_status"
                    )
                    existing_door.lock_rule = door_lock_rule.get("type", "")
                    existing_door.lock_rule_ended_time = door_lock_rule.get(
                        "ended_time", 0
                    )
                    _LOGGER.debug(
                        "Updated existing door, id: %s, name: %s, dps: %s, door_lock_relay_status: %s, door lock rule: %s, door lock rule ended time: %s using polling %s",
                        door_id,
                        door.get("name"),
                        door.get("door_position_status"),
                        door.get("door_lock_relay_status"),
                        door_lock_rule.get("type", ""),
                        door_lock_rule.get("ended_time", 0),
                        self.use_polling,
                    )
                else:
                    self._doors[door_id] = UnifiAccessDoor(
                        door_id=door["id"],
                        name=normalize_door_name(door["name"]),
                        door_position_status=door.get("door_position_status"),
                        door_lock_relay_status=door.get("door_lock_relay_status"),
                        door_lock_rule=door_lock_rule.get("type", ""),
                        door_lock_rule_ended_time=door_lock_rule.get("ended_time", 0),
                        hub=self,
                    )
                    _LOGGER.debug(
                        "Found new door, id: %s, name: %s, dps: %s, door_lock_relay_status: %s, door lock rule: %s, door lock rule ended time: %s, using polling: %s",
                        door_id,
                        door.get("name"),
                        door.get("door_position_status"),
                        door.get("door_lock_relay_status"),
                        door_lock_rule.get("type", ""),
                        door_lock_rule.get("ended_time", 0),
                        self.use_polling,
                    )
            else:
                _LOGGER.debug("Door %s is not bound to a hub. Ignoring", door)

        # ✅ Start websocket regardless of polling - we need it for access logs/events
        if self.update_t is None:
            _LOGGER.debug("Starting continuous updates (websocket)")
            self.start_continuous_updates()

        _LOGGER.debug("Got doors %s", self.doors)
        return self._doors

    def authenticate(self, api_token: str) -> str:
        """Test if we can authenticate with the host."""
        self.set_api_token(api_token)
        _LOGGER.info("Authenticating %s", self.host)
        try:
            self.update()
        except ApiError:
            _LOGGER.error("Could perform action with %s. Check host and token", self.host)
            return "api_error"
        except ApiAuthError:
            _LOGGER.error("Could not authenticate with %s. Check host and token", self.host)
            return "api_auth_error"
        except SSLError:
            _LOGGER.error("Error validating SSL Certificate for %s", self.host)
            return "ssl_error"
        except ConnError:
            _LOGGER.error("Cannot connect to %s", self.host)
            return "cannot_connect"

        return "ok"

    def get_door_lock_rule(self, door_id: str) -> DoorLockRuleStatus:
        """Get door lock rule."""
        _LOGGER.debug("Getting door lock rule for door_id %s", door_id)
        try:
            data = self._make_http_request(
                f"{self.host}{DOOR_LOCK_RULE_URL}".format(door_id=door_id)
            )
            _LOGGER.debug("Got door lock rule for door_id %s %s", door_id, data)
            return cast(DoorLockRuleStatus, data)
        except (ApiError, KeyError):
            self.supports_door_lock_rules = False
            _LOGGER.debug("cannot get door lock rule. Likely unsupported hub")
            return {"type": "", "ended_time": 0}

    def set_door_lock_rule(self, door_id: str, door_lock_rule: DoorLockRule) -> None:
        """Set door lock rule."""
        _LOGGER.info("Setting door lock rule for Door ID %s %s", door_id, door_lock_rule)
        self._make_http_request(
            f"{self.host}{DOOR_LOCK_RULE_URL}".format(door_id=door_id),
            "PUT",
            door_lock_rule,
        )

    def get_doors_emergency_status(self) -> EmergencyData:
        """Get doors emergency status."""
        _LOGGER.debug("Getting doors emergency status")
        data = self._make_http_request(f"{self.host}{DOORS_EMERGENCY_URL}")
        self.evacuation = data["evacuation"]
        self.lockdown = data["lockdown"]
        _LOGGER.debug("Got doors emergency status %s", data)
        return data

    def set_doors_emergency_status(self, emergency_data: EmergencyData) -> None:
        """Set doors emergency status."""
        _LOGGER.info("Setting doors emergency status %s", emergency_data)
        self._make_http_request(f"{self.host}{DOORS_EMERGENCY_URL}", "PUT", emergency_data)
        self.evacuation = emergency_data.get("evacuation", self.evacuation)
        self.lockdown = emergency_data.get("lockdown", self.lockdown)
        _LOGGER.debug("Emergency status set %s", emergency_data)

    def unlock_door(self, door_id: str) -> None:
        """Unlock a door via API."""
        _LOGGER.info("Unlocking door with id %s", door_id)
        self._make_http_request(
            f"{self.host}{DOOR_UNLOCK_URL}".format(door_id=door_id), "PUT"
        )

    def _make_http_request(self, url, method="GET", data=None) -> dict:
        """Make HTTP request to Unifi Access API server."""
        _LOGGER.debug("Making HTTP %s Request with URL %s and data %s", method, url, data)
        r = request(
            method,
            url,
            headers=self._http_headers,
            verify=self.verify_ssl,
            json=data,
            timeout=10,
        )

        if r.status_code == 401:
            raise ApiAuthError

        if r.status_code != 200:
            raise ApiError

        response = r.json()
        _LOGGER.debug("HTTP Response %s", response)
        return response["data"]

    def _get_thumbnail_image(self, url) -> bytes:
        """Get image from Unifi Access API server."""
        _LOGGER.debug("Getting thumbnail with URL %s", url)
        r = request(
            "GET",
            url=url,
            headers={"Authorization": f"Bearer {self._api_token}"},
            verify=self.verify_ssl,
            timeout=10,
        )

        if r.status_code == 401:
            raise ApiAuthError

        if r.status_code != 200:
            raise ApiError

        _LOGGER.debug("Got thumbnail response")
        return r.content

    def _handle_location_update_v2(self, update):
        """Process location update V2."""
        existing_door = None
        if update["data"]["location_type"] == "door":
            door_id = update["data"]["id"]
            _LOGGER.debug("access.data.v2.location.update: door id %s", door_id)
            if door_id in self.doors:
                existing_door = self.doors[door_id]
                _LOGGER.debug(
                    "updating location V2 for door name %s, id %s",
                    existing_door.name,
                    door_id,
                )
                if "state" in update["data"]:
                    existing_door.door_position_status = update["data"]["state"].get(
                        "dps", "close"
                    )
                    existing_door.door_lock_relay_status = (
                        "lock"
                        if update["data"]["state"].get("lock", "locked") == "locked"
                        else "unlock"
                    )
                    existing_door.lock_rule = ""
                    existing_door.lock_rule_ended_time = None
                    lock_rule = None
                    if "remain_lock" in update["data"]["state"]:
                        lock_rule = "remain_lock"
                    elif "remain_unlock" in update["data"]["state"]:
                        lock_rule = "remain_unlock"
                    if lock_rule:
                        existing_door.lock_rule = update["data"]["state"][lock_rule]["type"]
                        existing_door.lock_rule_ended_time = update["data"]["state"][lock_rule]["until"]

                if "thumbnail" in update["data"]:
                    try:
                        existing_door.thumbnail = self._get_thumbnail_image(
                            f"{self.host}{STATIC_URL}{update['data']['thumbnail']['url']}"
                        )
                        existing_door.thumbnail_last_updated = datetime.fromtimestamp(
                            update["data"]["thumbnail"]["door_thumbnail_last_update"]
                        )
                    except (ApiError, ApiAuthError):
                        _LOGGER.error("Could not get thumbnail for door id %s", door_id)
        return existing_door

    def on_message(self, ws: websocket.WebSocketApp, message):
        """Handle messages received on the websocket client."""
        event = None
        event_attributes = None
        event_done_callback = None

        if "Hello" not in message:
            _LOGGER.debug("websocket message received %s", message)
            update = json.loads(message)
            existing_door = None

            match update.get("event"):
                case "access.data.v2.location.update":
                    existing_door = self._handle_location_update_v2(update)

                case "access.remote_view":
                    door_name = update["data"]["door_name"]
                    normalized_door_name = normalize_door_name(door_name)
                    existing_door = next(
                        (
                            door
                            for door in self.doors.values()
                            if normalize_door_name(door.name) == normalized_door_name
                        ),
                        None,
                    )
                    if existing_door is not None:
                        existing_door.doorbell_request_id = update["data"]["request_id"]
                        event = "doorbell_press"
                        event_attributes = {
                            "door_name": existing_door.name,
                            "door_id": existing_door.id,
                            "type": DOORBELL_START_EVENT,
                        }

                case "access.remote_view.change":
                    doorbell_request_id = update["data"]["remote_call_request_id"]
                    existing_door = next(
                        (
                            door
                            for door in self.doors.values()
                            if door.doorbell_request_id == doorbell_request_id
                        ),
                        None,
                    )
                    if existing_door is not None:
                        existing_door.doorbell_request_id = None
                        event = "doorbell_press"
                        event_attributes = {
                            "door_name": existing_door.name,
                            "door_id": existing_door.id,
                            "type": DOORBELL_STOP_EVENT,
                        }

                case "access.data.device.update":
                    device_id = update["data"]["unique_id"]
                    device_type = update["data"]["device_type"]
                    door_id = update["data"].get("door", {}).get("unique_id")
                    if door_id in self.doors and self.doors[door_id].hub_id is None:
                        existing_door = self.doors[door_id]
                        existing_door.hub_type = device_type
                        existing_door.hub_id = device_id

                case "access.logs.add":
                    door = next(
                        (
                            target
                            for target in update["data"]["_source"]["target"]
                            if target["type"] == "door"
                        ),
                        None,
                    )
                    if door is not None:
                        door_id = door.get("id")
                        existing_door = self.doors.get(door_id)

                        # Bug workaround: sometimes door id is actually hub id
                        if existing_door is None:
                            existing_door = next(
                                (
                                    d
                                    for d in self.doors.values()
                                    if getattr(d, "hub_id", None) == door_id
                                ),
                                None,
                            )

                        if existing_door is not None:
                            src = update["data"]["_source"]

                            actor = (src.get("actor") or {}).get("display_name", "N/A")
                            authentication = (
                                (src.get("authentication") or {}).get("credential_provider")
                                or "UNKNOWN"
                            )
                            result = src.get("result") or "UNKNOWN"

                            device_config = next(
                                (
                                    target
                                    for target in src.get("target", [])
                                    if target["type"] == "device_config"
                                ),
                                None,
                            )
                            access_type = ""
                            if device_config is not None:
                                access_type = (device_config.get("display_name") or "").strip()

                            at = access_type.lower()
                            event_type = None
                            if "entry" in at:
                                event_type = ACCESS_ENTRY_EVENT
                            elif "exit" in at:
                                event_type = ACCESS_EXIT_EVENT

                            if event_type is not None:
                                event = "access"
                                event_attributes = {
                                    "door_name": existing_door.name,
                                    "door_id": existing_door.id,
                                    "actor": actor,
                                    "authentication": authentication,
                                    "result": result,
                                    "type": event_type,
                                }
                                _LOGGER.info(
                                    "Door %s (%s) %s by %s auth=%s result=%s",
                                    existing_door.name,
                                    existing_door.id,
                                    event_type,
                                    actor,
                                    authentication,
                                    result,
                                )

                case "access.hw.door_bell":
                    door_id = update["data"]["door_id"]
                    if door_id in self.doors:
                        existing_door = self.doors[door_id]
                        existing_door.doorbell_request_id = update["data"]["request_id"]
                        event = "doorbell_press"
                        event_attributes = {
                            "door_name": existing_door.name,
                            "door_id": existing_door.id,
                            "type": DOORBELL_START_EVENT,
                        }

                        async def on_complete(_fut):
                            existing_door.doorbell_request_id = None
                            stop_event = "doorbell_press"
                            stop_attrs = {
                                "door_name": existing_door.name,
                                "door_id": existing_door.id,
                                "type": DOORBELL_STOP_EVENT,
                            }
                            await asyncio.sleep(2)
                            await existing_door.trigger_event(stop_event, stop_attrs)

                        event_done_callback = on_complete

                case "access.data.setting.update":
                    self.evacuation = update["data"]["evacuation"]
                    self.lockdown = update["data"]["lockdown"]
                    asyncio.run_coroutine_threadsafe(self.publish_updates(), self.loop)

                case _:
                    _LOGGER.debug("unhandled websocket message %s", update.get("event"))

            if existing_door:
                asyncio.run_coroutine_threadsafe(existing_door.publish_updates(), self.loop)

                if event is not None and event_attributes is not None:
                    task = asyncio.run_coroutine_threadsafe(
                        existing_door.trigger_event(event, event_attributes),
                        self.loop,
                    )
                    if event_done_callback is not None:
                        task.add_done_callback(event_done_callback)

    def on_error(self, ws: websocket.WebSocketApp, error):
        """Handle errors in the websocket client."""
        _LOGGER.exception("Got websocket error %s", error)

    def on_open(self, ws: websocket.WebSocketApp):
        """Show message on connection."""
        _LOGGER.info("Websocket connection established")

    def on_close(self, ws: websocket.WebSocketApp, close_status_code, close_msg):
        """Handle websocket closures."""
        _LOGGER.error(
            "Websocket connection closed code: %s message: %s",
            close_status_code,
            close_msg,
        )
        sslopt: dict[Any, Any]
        if self.verify_ssl is False:
            sslopt = {"cert_reqs": ssl.CERT_NONE}
        ws.run_forever(sslopt=sslopt, reconnect=5)

    def start_continuous_updates(self):
        """Start listening for updates in a separate thread using websocket-client."""
        self.update_t = Thread(target=self.listen_for_updates)
        self.update_t.daemon = True
        self.update_t.start()
        _LOGGER.info("Started websocket client in a new thread")

    def listen_for_updates(self):
        """Create a websocket client and start listening for updates."""
        uri = f"{self.websocket_host}{DEVICE_NOTIFICATIONS_URL}"
        _LOGGER.info("Listening for updates on %s", uri)
        ws = websocket.WebSocketApp(
            uri,
            header=self._websocket_headers,
            on_message=self.on_message,
            on_error=self.on_error,
            on_open=self.on_open,
            on_close=self.on_close,
        )
        sslopt = {"cert_reqs": ssl.CERT_REQUIRED}
        if self.verify_ssl is False:
            sslopt = {"cert_reqs": ssl.CERT_NONE}
        ws.run_forever(sslopt=sslopt, reconnect=5)

    def register_callback(self, callback: Callable[[], None]) -> None:
        """Register callback, called when settings change."""
        self._callbacks.add(callback)

    def remove_callback(self, callback: Callable[[], None]) -> None:
        """Remove previously registered callback."""
        self._callbacks.discard(callback)

    async def publish_updates(self) -> None:
        """Schedule call all registered callbacks."""
        for callback in self._callbacks:
            callback()
