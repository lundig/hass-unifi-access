"""Platform for event integration."""

from __future__ import annotations

import asyncio
import logging

from propcache.api import cached_property

from homeassistant.components.event import EventDeviceClass, EventEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .const import (
    ACCESS_ENTRY_EVENT,
    ACCESS_EXIT_EVENT,
    DOMAIN,
    DOORBELL_START_EVENT,
    DOORBELL_STOP_EVENT,
)
from .door import UnifiAccessDoor
from .hub import UnifiAccessHub

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Add event entities for passed config entry."""
    hub: UnifiAccessHub = hass.data[DOMAIN][config_entry.entry_id]
    coordinator = hass.data[DOMAIN]["coordinator"]

    # Add event entities regardless of polling mode, so events exist in HA
    async_add_entities((AccessEventEntity(hass, door) for door in coordinator.data.values()))
    async_add_entities((DoorbellPressedEventEntity(hass, door) for door in coordinator.data.values()))


class AccessEventEntity(EventEntity):
    """Authorized User Event Entity."""

    _attr_event_types = [ACCESS_ENTRY_EVENT, ACCESS_EXIT_EVENT]
    _attr_has_entity_name = True

    @cached_property
    def should_poll(self) -> bool:
        return False

    def __init__(self, hass: HomeAssistant, door: UnifiAccessDoor) -> None:
        self.hass = hass
        self.door = door
        self._attr_unique_id = f"{self.door.id}_access"
        self._attr_translation_placeholders = {"door_name": self.door.name}

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self.door.id)},
            name=self.door.name,
            model="UAH",
            manufacturer="Unifi",
        )

    async def _async_process_event(self, event: str, event_attributes: dict[str, str]) -> None:
        """Process event on HA event loop."""
        _LOGGER.debug("Access event %s attrs=%s", event, event_attributes)

        # Update the EventEntity
        self._trigger_event(event, event_attributes)
        self.async_write_ha_state()

        # Also fire on HA event bus
        self.hass.bus.async_fire(event, event_attributes)

    def _async_handle_event(self, event: str, event_attributes: dict[str, str]) -> None:
        """Handle access events (safe across threads)."""
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None

        # If we're already on HA loop, run directly; otherwise schedule thread-safe
        if running_loop is self.hass.loop:
            self.hass.async_create_task(self._async_process_event(event, event_attributes))
        else:
            self.hass.loop.call_soon_threadsafe(
                self.hass.async_create_task,
                self._async_process_event(event, event_attributes),
            )

    async def async_added_to_hass(self) -> None:
        """Register event listener with door."""
        self.door.add_event_listener("access", self._async_handle_event)

    async def async_will_remove_from_hass(self) -> None:
        await super().async_will_remove_from_hass()
        self.door.remove_event_listener("access", self._async_handle_event)


class DoorbellPressedEventEntity(EventEntity):
    """Doorbell Press Event Entity."""

    _attr_device_class = EventDeviceClass.DOORBELL
    _attr_event_types = [DOORBELL_START_EVENT, DOORBELL_STOP_EVENT]
    _attr_translation_key = "doorbell_event"
    _attr_has_entity_name = True

    @cached_property
    def should_poll(self) -> bool:
        return False

    def __init__(self, hass: HomeAssistant, door: UnifiAccessDoor) -> None:
        self.hass = hass
        self.door = door
        self._attr_unique_id = f"{self.door.id}_doorbell_press"
        self._attr_translation_placeholders = {"door_name": self.door.name}

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self.door.id)},
            name=self.door.name,
            model=self.door.hub_type,
            manufacturer="Unifi",
        )

    async def _async_process_event(self, event: str, event_attributes: dict[str, str]) -> None:
        _LOGGER.debug("Doorbell event %s attrs=%s", event, event_attributes)
        self._trigger_event(event, event_attributes)
        self.async_write_ha_state()
        self.hass.bus.async_fire(event, event_attributes)

    def _async_handle_event(self, event: str, event_attributes: dict[str, str]) -> None:
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None

        if running_loop is self.hass.loop:
            self.hass.async_create_task(self._async_process_event(event, event_attributes))
        else:
            self.hass.loop.call_soon_threadsafe(
                self.hass.async_create_task,
                self._async_process_event(event, event_attributes),
            )

    async def async_added_to_hass(self) -> None:
        self.door.add_event_listener("doorbell_press", self._async_handle_event)

    async def async_will_remove_from_hass(self) -> None:
        await super().async_will_remove_from_hass()
        self.door.remove_event_listener("doorbell_press", self._async_handle_event)
