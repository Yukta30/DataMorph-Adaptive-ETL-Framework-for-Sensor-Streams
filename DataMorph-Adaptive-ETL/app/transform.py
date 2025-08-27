from typing import Dict, Any
import pandas as pd

def to_dataframe(batch):
    if not batch:
        return pd.DataFrame()
    return pd.DataFrame(batch)

def enrich(core: Dict[str, Any]) -> Dict[str, Any]:
    # Example derived metric: heat_index-ish placeholder
    t = core.get("temperature")
    h = core.get("humidity")
    if t is not None and h is not None:
        core["temp_x_humidity"] = float(t) * float(h)
    return core
