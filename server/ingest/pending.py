import json
import os
from typing import Dict
from ingest.domain import CaseRecord

DEFAULT_PATH = "ingest/pending_cases.json"

def load_pending(path: str = DEFAULT_PATH) -> dict:
    if not os.path.exists(path):
        return {}

    with open(path, "r") as f:
        raw = json.load(f)

    return {
        k: CaseRecord.from_dict(v)
        for k, v in raw.items()
    }


def write_pending(cases: dict, path: str = DEFAULT_PATH):
    with open(path, "w") as f:
        json.dump(
            {k: v.to_dict() for k, v in cases.items()},
            f,
            indent=2,
        )

def remove_processed(
    cases: Dict[str, CaseRecord],
    processed_keys: list[str],
) -> Dict[str, CaseRecord]:
    for key in processed_keys:
        cases.pop(key, None)
    return cases
