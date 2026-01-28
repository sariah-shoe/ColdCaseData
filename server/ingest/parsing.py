import os
import re
import pytesseract
from pdf2image import convert_from_path
from datetime import datetime
from ingest.domain import PDF_DIR, CaseRecord, Sex, Race

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

VICTIM_REGEX = rf"""
^\s*Victim\s*:?\s*
(?!long\s+thought)
(?!forgotten)
{LABEL_GUARD}
([A-Z][A-Za-z\-\s'.]+?)
(?=\s+(?:Age|Sex|Race|Date|Location)\b|$)
"""

AGE_REGEX = rf"Age\s*:?\s*{LABEL_GUARD}([^\n]+)"
SEX_REGEX = rf"Sex\s*:?\s*{LABEL_GUARD}([^\n]+)"
RACE_REGEX = rf"Race\s*:?\s*{LABEL_GUARD}([^\n]+)"
DATE_REGEX = r"Date\s*:?\s*(\d{1,2}\s*[/-]\s*\d{1,2}\s*[/-]\s*\d{4})"
LOCATION_REGEX = rf"Location\s*:?\s*{LABEL_GUARD}([^\n]+)"

def extract(pattern: str, text: str) -> str | None:
    match = match = re.search(
    pattern,
    text,
    re.MULTILINE | re.DOTALL | re.IGNORECASE | re.VERBOSE
)
    return match.group(1).strip() if match else None


def parseOne(session, case: CaseRecord):
    filepath = PDF_DIR / case.pdf_name
    if not os.path.isfile(filepath):
        case.warnings.append(f"File not found: {filepath}")
        return None

    image = convert_from_path(filepath, dpi=300)[0]
    text = pytesseract.image_to_string(image, lang="eng")

    
    case_number_raw = extract(CASE_REGEX, text)

    if not case_number_raw:
        case_number_raw = extract(CASE_FALLBACK_REGEX, text)

    if not case_number_raw:
        case.warnings.append(f"[MISSING REQUIREMENT WARNING] field=case_number")
        case_number_norm = None
    else:
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
        case.warnings.append(f"[NORMALIZATION WARNING] field=age raw={age_raw}")
        age_norm = None
    
    sex_map = {
    "Female": Sex.F,
    "Male": Sex.M,
    }
    
    sex_raw = extract(SEX_REGEX, text)
    sex_key = sex_raw.strip().title() if sex_raw else None
    
    if sex_key not in sex_map:
        case.warnings.append(f"[NORMALIZATION WARNING] field=sex raw={sex_key}")
        
    sex_norm = sex_map.get(sex_key, Sex.NA)
    
    race_map = {
    "White": Race.WHITE,
    "Black": Race.BLACK,
    "Hispanic": Race.HISPANIC,
    "Asian": Race.ASIAN,
    "Pacific Islander": Race.PACIFIC_ISLANDER,
    "Native American": Race.NATIVE_AMERICAN,
    "Caucasian": Race.WHITE,
    }
    
    race_raw = extract(RACE_REGEX, text)
    race_key = race_raw.strip().title() if race_raw else None
    
    if race_key not in race_map:
        case.warnings.append(f"[NORMALIZATION WARNING] field=race raw={race_key}")
    
    race_norm = race_map.get(race_key, Race.OTHER)

    incident_date_raw = extract(DATE_REGEX, text)
    if not incident_date_raw:
        case.warnings.append(f"[MISSING REQUIREMENT WARNING] field=date")
        incident_date_norm = None
    else:
        incident_date_clean = incident_date_raw.replace(" ", "").replace("-", "/")
        incident_date_norm = datetime.strptime(incident_date_clean, "%m/%d/%Y").date()     
    
    location_raw = extract(LOCATION_REGEX, text)
    if not location_raw:
        case.warnings.append(f"[MISSING REQUIREMENT WARNING] field=location")
        location_norm = None
    else:
        location_norm = location_raw.strip()
    
    synopsis_raw = extract(
        r"Synopsis\s*:?\s*(.*?)\n\n",
        text
    )
    synopsis_norm = re.sub(r"\s+", " ", synopsis_raw).strip() if synopsis_raw else None
    
    case.case_number = case_number_norm
    case.victim = victim_norm
    case.age = age_norm
    case.sex = sex_norm
    case.race = race_norm
    case.incident_date = incident_date_norm
    case.location = location_norm
    case.synopsis = synopsis_norm
    
    return case