import json
import os
from typing import Dict, TypedDict

class CaseRecord(TypedDict):
    url: str
    name: str
    source_status: str

DEFAULT_PATH = "ingest/pending_cases.json"


def load_pending(path: str = DEFAULT_PATH) -> Dict[str, CaseRecord]:
    if not os.path.exists(path):
        return {}

    with open(path, "r") as f:
        return json.load(f)


def write_pending(cases: Dict[str, CaseRecord], path: str = DEFAULT_PATH) -> None:
    with open(path, "w") as f:
        json.dump(cases, f, indent=2)


def remove_processed(
    cases: Dict[str, CaseRecord],
    processed_keys: list[str],
) -> Dict[str, CaseRecord]:
    for key in processed_keys:
        cases.pop(key, None)
    return cases
