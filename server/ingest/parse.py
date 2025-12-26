import json
import sys
import os
import re
from db import SessionLocal
from db.models import ColdCase
from ingest.pending import load_pending, remove_processed, write_pending
from sqlalchemy.dialects.postgresql import insert
from pdf2image import convert_from_path
from tqdm import tqdm
from datetime import datetime
import pytesseract
from typing import Dict, TypedDict, Optional

class CaseRecord(TypedDict):
    url: str
    name: str
    source_status: str

# Global variables for file management
PDF_DIR = "coldCasePDFs"

def extract(pattern: str, text: str) -> str | None:
    match = match = re.search(
    pattern,
    text,
    re.MULTILINE | re.DOTALL | re.IGNORECASE | re.VERBOSE
)
    return match.group(1).strip() if match else None

def warn_default(field: str, raw_value: str, case_number: str):
    print(
        f"[NORMALIZATION WARNING] "
        f"case={case_number} field={field} raw='{raw_value}'"
    )
    
def upsert_cold_case(session, parsed_case: dict):
    stmt = insert(ColdCase).values(**parsed_case)

    update_cols = {
        col: stmt.excluded[col]
        for col in parsed_case
        if col != "case_number" and parsed_case[col] is not None
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=["case_number"],
        set_=update_cols
    )

    session.execute(stmt)

def parseOne(session, case: CaseRecord):
    filepath = os.path.join(PDF_DIR, case["name"])
    if not os.path.isfile(filepath):
        print(f"File not found: {filepath}")
        return False

    image = convert_from_path(filepath, dpi=300)[0]
    text = pytesseract.image_to_string(image, lang="eng")
    
    print(text)
    
    LABEL_GUARD = r"(?!\b(?:Case|Date|Location|Victim|Age|Sex|Race|Synopsis)\b)"
    CASE_REGEX = r"""
        Case\s*
        [#|,]?\s*        # optional #, |, or comma
        :?\s*
        (
            \d{2,4}              # year
            [\-\u2013\s]         # separator
            \d+                  # id
        )
    """
    CASE_FALLBACK_REGEX = r"""
    ^
    \s*
    (
        \d{4}
        -
        \d{5,6}
    )
    \s*$
    """

    VICTIM_REGEX = rf"Victim\s*:?\s*{LABEL_GUARD}([^\n]+)"
    AGE_REGEX = rf"Age\s*:?\s*{LABEL_GUARD}([^\n]+)"
    SEX_REGEX = rf"Sex\s*:?\s*{LABEL_GUARD}([^\n]+)"
    RACE_REGEX = rf"Race\s*:?\s*{LABEL_GUARD}([^\n]+)"
    DATE_REGEX = r"Date\s*:?\s*(\d{1,2}\s*[/-]\s*\d{1,2}\s*[/-]\s*\d{4})"
    LOCATION_REGEX = rf"Location\s*:?\s*{LABEL_GUARD}([^\n]+)"
    
    status = case["source_status"]
    
    case_number_raw = extract(CASE_REGEX, text)

    if not case_number_raw:
        case_number_raw = extract(CASE_FALLBACK_REGEX, text)

    if not case_number_raw:
        print("Case number is required and was not found")
        return False
        
    case_number_norm = (
    case_number_raw
        .replace("\u2013", "-")  # en dash → hyphen
        .replace(" ", "-")       # space → hyphen
    )
    
    victim_raw = extract(VICTIM_REGEX, text)
    victim_norm = victim_raw.strip().title() if victim_raw else None
    
    age_raw = extract(AGE_REGEX, text)
    try:
        age_norm = int(age_raw) if age_raw else None
    except (TypeError, ValueError):
        age_norm = None
    
    sex_map = {
        "Female" : "F",
        "Male" : "M"
    }
    
    sex_raw = extract(SEX_REGEX, text)
    sex_key = sex_raw.strip().title() if sex_raw else None
    
    if sex_key not in sex_map:
        warn_default("sex", sex_raw, case_number_norm)
        
    sex_norm = sex_map.get(sex_key, "N/A")
    
    race_map = {
        "White": "White",
        "Black": "Black",
        "Hispanic": "Hispanic",
        "Asian": "Asian",
        "Pacific Islander": "Pacific Islander",
        "Native American": "Native American",
        "Caucasian": "White",
    }
    
    race_raw = extract(RACE_REGEX, text)
    race_key = race_raw.strip().title() if race_raw else None
    
    if race_key not in race_map:
        warn_default("race", race_raw, case_number_norm)
    
    race_norm = race_map.get(race_key, "Other")

    incident_date_raw = extract(DATE_REGEX, text)
    if not incident_date_raw:
        print("Incident date required and was not found")
        return False
    
    incident_date_clean = incident_date_raw.replace(" ", "").replace("-", "/")
    incident_date_norm = datetime.strptime(incident_date_clean, "%m/%d/%Y").date()     
    
    location_raw = extract(LOCATION_REGEX, text)
    if not location_raw:
        print("Location required and was not found")
        return False
    
    synopsis_raw = extract(
        r"Synopsis\s*:?\s*(.*?)\n\n",
        text
    )
    synopsis_norm = re.sub(r"\s+", " ", synopsis_raw).strip() if synopsis_raw else None
    
    parsed_case = {
        "case_number": case_number_norm,
        "victim": victim_norm,
        "age": age_norm,
        "sex": sex_norm,
        "race": race_norm,
        "incident_date": incident_date_norm,
        "location": location_raw.strip(),
        "synopsis": synopsis_norm,
        "status" : status,
    }

    upsert_cold_case(session, parsed_case)
    return True
    
def parseAllPDFs():
    session = SessionLocal()
    try:
        cases = load_pending()
        processed_keys = []

        for key, case in tqdm(cases.items()):
            success = parseOne(session, case)
            if success:
                processed_keys.append(key)

        session.commit()

        # Remove successfully processed cases
        remove_processed(cases, processed_keys)
        write_pending(cases)
        
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
