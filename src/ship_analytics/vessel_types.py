"""Normalize AIS ship-type metadata and provide a stable fallback when it is absent."""

from __future__ import annotations

_KNOWN_TYPES = {"Cargo", "Tanker", "Passenger", "Fishing", "Military", "Pleasure", "Tug"}
_FALLBACK_TYPES = ("Cargo", "Tanker", "Passenger", "Fishing", "Tug")


def normalize_vessel_type(value: object | None, mmsi: int) -> str:
    """Return an AIS type when present, otherwise a deterministic demo classification.

    AIS feeds frequently omit ship metadata on position reports. The fallback is
    deterministic per MMSI (rather than random) so a vessel does not change
    type between dashboard refreshes; it is only used when the source gives no
    usable classification.
    """
    raw = str(value or "").strip()
    if raw in _KNOWN_TYPES:
        return raw
    lowered = raw.lower()
    for name in _KNOWN_TYPES:
        if name.lower() in lowered:
            return name
    if raw.isdigit():
        code = int(raw)
        if 70 <= code <= 79 or 80 <= code <= 89:
            return "Cargo" if code < 80 else "Tanker"
        if 60 <= code <= 69:
            return "Passenger"
        if 30 <= code <= 39:
            return "Fishing"
        if 50 <= code <= 59:
            return "Tug"
    return _FALLBACK_TYPES[mmsi % len(_FALLBACK_TYPES)]
