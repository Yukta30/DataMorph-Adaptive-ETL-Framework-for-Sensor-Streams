from typing import Dict, Any, Tuple
from datetime import datetime

# Simple, dynamic validator that coalesces known fields and preserves extras.
KNOWN_FIELDS = {
    "device_id": str,
    "ts": "datetime",  # ISO8601 string or epoch
    "temperature": float,
    "humidity": float,
}

def coerce_ts(value) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value)
    if isinstance(value, str):
        try:
            # Try ISO8601
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except Exception:
            # Try numeric string
            try:
                return datetime.fromtimestamp(float(value))
            except Exception:
                pass
    raise ValueError(f"Invalid ts: {value}")

def validate_record(rec: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any], list]:
    """Return (core, extras, warnings). Core is normalized, extras preserved."""
    core = {}
    extras = {}
    warnings = []

    for k, v in rec.items():
        if k in KNOWN_FIELDS:
            expected = KNOWN_FIELDS[k]
            try:
                if expected is float:
                    core[k] = float(v) if v is not None else None
                elif expected is int:
                    core[k] = int(v) if v is not None else None
                elif expected is str:
                    core[k] = str(v) if v is not None else None
                elif expected == "datetime":
                    core[k] = coerce_ts(v)
                else:
                    core[k] = v
            except Exception as e:
                warnings.append(f"Field {k} failed coercion: {e}")
                core[k] = None
        else:
            extras[k] = v
    # Required minimal fields
    if "device_id" not in core or not core["device_id"]:
        warnings.append("Missing device_id")
    if "ts" not in core or core["ts"] is None:
        warnings.append("Missing ts")
    return core, extras, warnings
