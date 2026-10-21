"""Daily Briefing assembler.

Builds a morning summary from weather (Open-Meteo, no API key), today's calendar
events, and unread email. Calendar/email use the existing Google tools and are
best-effort — if the Google account isn't connected, those sections are skipped
rather than failing the whole briefing.
"""

import logging
from datetime import datetime

import httpx

logger = logging.getLogger("briefing")

_WMO = {
    0: "clear sky", 1: "mainly clear", 2: "partly cloudy", 3: "overcast",
    45: "fog", 48: "rime fog", 51: "light drizzle", 53: "drizzle", 55: "dense drizzle",
    61: "light rain", 63: "rain", 65: "heavy rain", 71: "light snow", 73: "snow",
    75: "heavy snow", 80: "rain showers", 81: "rain showers", 82: "violent rain showers",
    95: "thunderstorm", 96: "thunderstorm w/ hail", 99: "severe thunderstorm",
}


async def _weather(city: str) -> str:
    city = str(city or "").strip()
    if not city:
        return ""
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            g = await c.get(
                "https://geocoding-api.open-meteo.com/v1/search",
                params={"name": city, "count": 1},
            )
            results = (g.json() or {}).get("results") or []
            if not results:
                return f"Weather: couldn't find '{city}'."
            loc = results[0]
            w = await c.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": loc["latitude"], "longitude": loc["longitude"],
                    "current": "temperature_2m,weather_code",
                    "daily": "temperature_2m_max,temperature_2m_min",
                    "timezone": "auto", "forecast_days": 1,
                },
            )
            wj = w.json() or {}
        cur = wj.get("current", {})
        daily = wj.get("daily", {})
        temp = cur.get("temperature_2m")
        desc = _WMO.get(cur.get("weather_code"), "")
        hi = (daily.get("temperature_2m_max") or [None])[0]
        lo = (daily.get("temperature_2m_min") or [None])[0]
        bits = [f"**Weather in {loc.get('name', city)}:** {temp}°C"]
        if desc:
            bits.append(desc)
        if hi is not None and lo is not None:
            bits.append(f"(high {hi}°, low {lo}°)")
        return " ".join(bits)
    except Exception as exc:
        logger.warning("weather failed: %s", exc)
        return ""


async def build_briefing(city: str = "", first_name: str = "", google_creds=None) -> str:
    """`google_creds` are the briefing owner's own Google credentials (None if they
    haven't connected Google). The Google tools read them from a per-request context,
    so they must be set here; a background tick has no chat request to set them."""
    hello = f"Good morning{', ' + first_name if first_name else ''}!"
    parts = [f"{hello} Here's your briefing for **{datetime.now().strftime('%A, %B %d')}**."]

    weather = await _weather(city)
    if weather:
        parts.append(weather)

    if google_creds is not None:
        from tools import tool_registry, google_auth
        google_auth.set_user_credentials(google_creds)
        try:
            # Calendar + email via the registered tools (best-effort).
            try:
                events = await tool_registry.execute(
                    "calendar", {"command_string": "action:list_events; days:1; limit:8"}
                )
                if events and "Error" not in str(events)[:12]:
                    parts.append(f"**Today's schedule:**\n{events}")
            except Exception as exc:
                logger.info("briefing calendar skipped: %s", exc)

            try:
                unread = await tool_registry.execute(
                    "email", {"command_string": "read:true; query:is:unread; limit:5"}
                )
                if unread and "Error" not in str(unread)[:12]:
                    parts.append(f"**Unread email:**\n{unread}")
            except Exception as exc:
                logger.info("briefing email skipped: %s", exc)
        finally:
            google_auth.clear_user_credentials()

    if len(parts) == 1:
        parts.append("_Connect Google and set your city to see schedule, email, and weather here._")
    return "\n\n".join(parts)
