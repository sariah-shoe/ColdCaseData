"""
repair.py — human-in-the-loop repair tool

Features:
- Repair pending parse failures (pending.json)
- Repair suspicious DB records
- Safe commits: never silently overwrites cleaned DB data
- Required-field validation loop:
    If still missing required fields -> Edit again / Discard / Skip
- DB vs OCR compare + actions:
    Discard OCR / Replace DB / Merge selected fields
- Editor-based synopsis editing with fallbacks (vim/vi/nano) + inline patch mode

"""

import os
import re
import shutil
import subprocess
import tempfile
import copy
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple, Literal
from tqdm import tqdm


# =============================================================================
# PROJECT IMPORTS (adjust these if your paths differ)
# =============================================================================

# CaseRecord + Enums
from ingest.domain import CaseRecord, Sex, Race, CaseStatus

# Pending helpers
from ingest.pending import load_pending, write_pending  # type: ignore

# DB session creation
from db.engine import SessionLocal

# DB query/apply functions (preferred)
from ingest.queries import find_case, find_quality_candidates
from ingest.crupdate import insert_new_case, replace_case, merge_case_fields

# =============================================================================
# CONFIG
# =============================================================================

REQUIRED_FIELDS = ("case_number", "incident_date", "location")

EDITABLE_FIELDS: Dict[str, str] = {
    "case_number": "text",
    "victim": "text",
    "age": "int",
    "sex": "enum_sex",
    "race": "enum_race",
    "incident_date": "date",
    "location": "text",
    "synopsis": "text_long",
    "status": "enum_status",
}

DATE_FORMATS = ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%m.%d.%Y")

EditOutcome = Literal["done", "discard", "skip"]

# =============================================================================
# UTILITIES
# =============================================================================

def _safe_enum_value(x: Any) -> Any:
    # If x is an Enum, return x.value; else return x.
    return getattr(x, "value", x)


def _record_to_dict(record: Any) -> Dict[str, Any]:
    """
    Convert CaseRecord (dataclass/typed object) -> dict, safely serializable.
    """
    if record is None:
        return {}
    if hasattr(record, "to_dict") and callable(record.to_dict):
        d = record.to_dict()
    elif is_dataclass(record):
        d = asdict(record)
    elif isinstance(record, dict):
        d = dict(record)
    else:
        # last resort: attribute scrape
        d = {k: getattr(record, k) for k in EDITABLE_FIELDS.keys() if hasattr(record, k)}

    # normalize enums + dates
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, (datetime, date)):
            out[k] = v.isoformat()
        else:
            out[k] = _safe_enum_value(v)
    return out


def _getattr(record: Any, field: str) -> Any:
    if isinstance(record, dict):
        return record.get(field)
    return getattr(record, field, None)


def _setattr(record: Any, field: str, value: Any) -> None:
    if isinstance(record, dict):
        record[field] = value
    else:
        setattr(record, field, value)


def prompt_yes_no(msg: str, default: Optional[bool] = None) -> bool:
    suffix = " [y/n]: "
    if default is True:
        suffix = " [Y/n]: "
    elif default is False:
        suffix = " [y/N]: "
    while True:
        raw = input(msg + suffix).strip().lower()
        if not raw and default is not None:
            return default
        if raw in ("y", "yes"):
            return True
        if raw in ("n", "no"):
            return False
        print("Please enter y or n.")


def prompt_int(msg: str, allow_blank: bool = True) -> Optional[int]:
    while True:
        raw = input(msg).strip()
        if raw == "" and allow_blank:
            return None
        try:
            return int(raw)
        except ValueError:
            print("Invalid integer.")


def prompt_text(msg: str, allow_blank: bool = True) -> Optional[str]:
    raw = input(msg).rstrip("\n")
    if raw.strip() == "" and allow_blank:
        return None
    return raw


def prompt_choice(msg: str, options: Sequence[str]) -> str:
    """
    options: list of valid string tokens
    """
    while True:
        raw = input(msg).strip()
        if raw in options:
            return raw
        print(f"Invalid choice. Choose one of: {', '.join(options)}")


def parse_date_input(raw: str) -> Optional[date]:
    raw = raw.strip()
    if raw == "":
        return None

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue

    # Last attempt: try to normalize separators/spaces like "10 / 25 / 2004"
    raw2 = re.sub(r"\s+", "", raw)
    raw2 = raw2.replace("\\", "/")
    for fmt in ("%m/%d/%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(raw2, fmt).date()
        except ValueError:
            continue

    return None


def missing_required_fields(record: Any) -> List[str]:
    missing = []
    for f in REQUIRED_FIELDS:
        v = _getattr(record, f)
        if v is None or (isinstance(v, str) and v.strip() == ""):
            missing.append(f)
    return missing

def prompt_missing_action(missing: List[str]) -> str:
    print("\nCannot commit: missing required fields:")
    for f in missing:
        print(f" - {f}")
    print("\nWhat would you like to do?")
    print("1) Edit again")
    print("2) Discard changes (leave record pending)")
    print("3) Skip this record for now")
    return prompt_choice("Select option: ", ["1", "2", "3"])


def pretty_print_record(record: Any) -> None:
    # Default to __repr__ if it exists (your CaseRecord prints nicely), else print dict
    try:
        print(record)
    except Exception:
        print(_record_to_dict(record))


# =============================================================================
# TEXT EDITING (EDITOR + FALLBACKS)
# =============================================================================

def _pick_editor() -> Optional[str]:
    # Prefer $EDITOR, then common CLI editors
    candidates = [
        os.environ.get("EDITOR"),
        "vim",
        "vi",
        "nano",
    ]
    for c in candidates:
        if c and shutil.which(c):
            return c
    return None


def edit_text_in_editor(initial_text: str) -> str:
    """
    Opens the text in a temp file using a terminal editor (vim/vi/nano).
    Falls back to inline patch mode if no editor exists.
    """
    editor = _pick_editor()
    if not editor:
        print("\n[WARN] No editor found in container (set $EDITOR or install vim/nano).")
        print("Falling back to inline patch mode.\n")
        return patch_text_inline(initial_text)

    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False, mode="w", encoding="utf-8") as tf:
        tf.write(initial_text or "")
        tf.flush()
        temp_name = tf.name

    try:
        subprocess.call([editor, temp_name])
        with open(temp_name, "r", encoding="utf-8") as f:
            edited = f.read()
        return edited
    finally:
        try:
            os.unlink(temp_name)
        except OSError:
            pass


def patch_text_inline(text: str) -> str:
    """
    Lightweight replace-loop for environments without editors.
    """
    current = text or ""
    while True:
        preview = current if len(current) <= 900 else current[:900] + "\n... (truncated) ..."
        print("\n--- Current text preview ---")
        print(preview)
        print("----------------------------")
        print("1) Replace substring")
        print("2) Append text")
        print("3) Finish")
        choice = prompt_choice("Select option: ", ["1", "2", "3"])
        if choice == "1":
            old = input("Find: ")
            new = input("Replace with: ")
            current = current.replace(old, new)
        elif choice == "2":
            add = input("Append: ")
            current += add
        else:
            return current

# =============================================================================
# EDITING
# =============================================================================

def edit_field(record: Any, field: str) -> None:
    ftype = EDITABLE_FIELDS[field]
    current = _getattr(record, field)

    # Print current value
    print(f"\n{field}: {current}")

    if ftype == "text":
        val = prompt_text("Enter new value (blank to keep): ", allow_blank=True)
        if val is not None:
            _setattr(record, field, val.strip() if isinstance(val, str) else val)

    elif ftype == "int":
        raw = input("Enter new integer (blank to keep): ").strip()
        if raw == "":
            return
        try:
            iv = int(raw)
            _setattr(record, field, iv)
        except ValueError:
            print("Invalid integer.")

    elif ftype == "date":
        raw = input("Enter new date (YYYY-MM-DD or MM/DD/YYYY; blank to keep): ").strip()
        if raw == "":
            return
        d = parse_date_input(raw)
        if not d:
            print("Invalid date format.")
            return
        _setattr(record, field, d)

    elif ftype == "enum_sex":
        print("1) M\n2) F\n3) N/A\n0) Keep current")
        choice = prompt_choice("Select option: ", ["1", "2", "3", "0"])
        if choice == "0":
            return
        mapping = {"1": Sex.M, "2": Sex.F, "3": Sex.NA}
        _setattr(record, field, mapping[choice])

    elif ftype == "enum_race":
        # Adjust based on your Race enum
        # common set (including Native American if you added it)
        options: List[Tuple[str, Any]] = []
        for idx, r in enumerate(list(Race), start=1):
            options.append((str(idx), r))
            print(f"{idx}) {r.value}")
        print("0) Keep current")
        choice = prompt_choice("Select option: ", [o[0] for o in options] + ["0"])
        if choice == "0":
            return
        selected = next(r for (k, r) in options if k == choice)
        _setattr(record, field, selected)

    elif ftype == "enum_status":
        options: List[Tuple[str, Any]] = []
        for idx, s in enumerate(list(CaseStatus), start=1):
            options.append((str(idx), s))
            print(f"{idx}) {s.value}")
        print("0) Keep current")
        choice = prompt_choice("Select option: ", [o[0] for o in options] + ["0"])
        if choice == "0":
            return
        selected = next(s for (k, s) in options if k == choice)
        _setattr(record, field, selected)

    elif ftype == "text_long":
        # Let user choose: editor, quick patches, or inline replace
        print("\n1) Open in editor")
        print("2) Inline patch mode")
        print("0) Keep current")
        choice = prompt_choice("Select option: ", ["1", "2", "0"])
        if choice == "0":
            return
        current_text = (current or "")
        if choice == "1":
            edited = edit_text_in_editor(current_text)
            _setattr(record, field, edited)
        elif choice == "2":
            patched = patch_text_inline(current_text)
            _setattr(record, field, patched)

    else:
        print(f"[WARN] Unknown field type {ftype}")


def edit_record_loop(record: Any) -> EditOutcome:
    """
    Interactive edit loop.

    Returns:
      - "done"    -> user finished and required fields are satisfied
      - "discard" -> user chose discard (record is reverted to original snapshot)
      - "skip"    -> user chose skip (record is reverted to original snapshot)
    """
    original = copy.deepcopy(record)

    while True:
        print("\nCurrent record:")
        pretty_print_record(record)

        print("\nEditable fields:")
        field_names = list(EDITABLE_FIELDS.keys())
        for i, f in enumerate(field_names, start=1):
            print(f"{i}. {f}")
        print("0. Done editing")

        valid_choices = [str(i) for i in range(0, len(field_names) + 1)]
        sel = prompt_choice("Select field to edit: ", valid_choices)

        if sel == "0":
            missing = missing_required_fields(record)
            if missing:
                action = prompt_missing_action(missing)
                if action == "1":
                    # keep editing
                    continue
                if action == "2":
                    print("Discarding changes.")
                    # revert changes
                    _restore_record(record, original)
                    return "discard"
                if action == "3":
                    print("Skipping for now.")
                    # revert changes
                    _restore_record(record, original)
                    return "skip"
            else:
                return "done"

        # edit selected field
        idx = int(sel) - 1
        field = field_names[idx]
        edit_field(record, field)


def _restore_record(target: Any, source: Any) -> None:
    """
    Restore dataclass-like object's fields from another instance.
    Works for dataclasses / simple objects with __dict__.
    """
    # Prefer __dict__ copy so we keep the same object identity
    if hasattr(target, "__dict__") and hasattr(source, "__dict__"):
        target.__dict__.clear()
        target.__dict__.update(copy.deepcopy(source.__dict__))
    else:
        # Fallback: best-effort attribute copy
        for name in dir(source):
            if name.startswith("_"):
                continue
            try:
                setattr(target, name, copy.deepcopy(getattr(source, name)))
            except Exception:
                pass


# =============================================================================
# DB APPLY (SAFE, HUMAN-CONFIRM)
# =============================================================================

def db_record_to_case_record(db_rec) -> CaseRecord:
    return CaseRecord(
        url="",               # DB rows don’t have this
        pdf_name="",          # DB rows don’t have this
        status=CaseStatus(db_rec.status),

        case_number=db_rec.case_number,
        victim=db_rec.victim,
        age=db_rec.age,
        sex=Sex(db_rec.sex) if db_rec.sex else None,
        race=Race(db_rec.race) if db_rec.race else None,
        incident_date=db_rec.incident_date,
        location=db_rec.location,
        synopsis=db_rec.synopsis,
        has_existing_record=True,
        warnings=[],
    )

def compare_records(db_rec: Any, ocr_rec: Any) -> List[str]:
    diffs = []
    for f in EDITABLE_FIELDS.keys():
        dbv = _getattr(db_rec, f) if not isinstance(db_rec, dict) else db_rec.get(f)
        ocrv = _getattr(ocr_rec, f)
        # normalize enum values for compare
        dbv_n = _safe_enum_value(dbv)
        ocrv_n = _safe_enum_value(ocrv)
        if isinstance(dbv_n, (date, datetime)):
            dbv_n = dbv_n.isoformat()
        if isinstance(ocrv_n, (date, datetime)):
            ocrv_n = ocrv_n.isoformat()
        if dbv_n != ocrv_n:
            diffs.append(f)
    return diffs


def show_side_by_side(db_rec: Any, ocr_rec: Any, fields: Sequence[str]) -> None:
    for f in fields:
        dbv = _getattr(db_rec, f) if not isinstance(db_rec, dict) else db_rec.get(f)
        ocrv = _getattr(ocr_rec, f)
        print(f"\nFIELD: {f}")
        print(f"DB : {dbv}")
        print(f"OCR: {ocrv}")


def apply_new_insert(session: Any, record: Any) -> None:
    if insert_new_case:
        insert_new_case(session, record)
        session.commit()
        return


def apply_replace(session: Any, record: Any) -> None:
    if replace_case:
        replace_case(session, record)
        session.commit()
        return


def apply_merge(session: Any, case_number: str, updates: Dict[str, Any]) -> None:
    if not updates:
        print("No updates selected.")
        return
    if merge_case_fields:
        merge_case_fields(session, case_number, updates)
        session.commit()
        return


def validate_and_commit_flow(session: Any, record: Any) -> bool:
    """
    Returns True if this pending item should be considered "processed" (and removed from pending).
    Returns False if it should remain pending (user skipped/discarded / couldn't commit).

    Uses edit_record_loop(record) which returns: "done" | "discard" | "skip"
    """

    def ensure_case_number(rec: Any) -> Optional[str]:
        cn = _getattr(rec, "case_number")
        if cn and str(cn).strip():
            return str(cn).strip()

        raw = input("Case number missing. Enter case number (or blank to skip): ").strip()
        if not raw:
            return None
        _setattr(rec, "case_number", raw)
        return raw

    def require_valid_or_offer_edit(rec: Any) -> bool:
        """
        Enforce required fields; if missing, offer editing loop.
        Returns True if record is valid, False if user discards/skips.
        """
        while True:
            missing = missing_required_fields(rec)
            if not missing:
                return True

            print("\nCannot commit: missing required fields:")
            for m in missing:
                print(f" - {m}")

            yn = prompt_yes_no("Do you want to edit this record now?", default=True)
            if not yn:
                print("Leaving record in pending.")
                return False

            outcome = edit_record_loop(rec)  # "done" | "discard" | "skip"
            if outcome in ("discard", "skip"):
                print("Leaving record in pending.")
                return False
            # if "done", loop will re-check missing and either pass or continue

    def choose_fields_to_merge(diff_fields: List[str]) -> List[str]:
        if not diff_fields:
            return []

        print("\nFields that differ (DB vs OCR):")
        for i, f in enumerate(diff_fields, start=1):
            print(f"{i}) {f}")

        print("\nSelect fields to apply from OCR to DB:")
        print(" - Enter comma-separated numbers (e.g. 1,3,4)")
        print(" - Enter 'a' to apply ALL differing fields")
        print(" - Enter blank to apply NONE")

        raw = input("Your choice: ").strip().lower()
        if raw == "":
            return []
        if raw == "a":
            return diff_fields

        chosen: List[str] = []
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        for p in parts:
            if not p.isdigit():
                continue
            idx = int(p)
            if 1 <= idx <= len(diff_fields):
                chosen.append(diff_fields[idx - 1])

        # de-dupe while preserving order
        seen = set()
        out: List[str] = []
        for f in chosen:
            if f not in seen:
                seen.add(f)
                out.append(f)
        return out

    # ------------------------------------------------------------------
    # 1) Ensure we have a case number
    # ------------------------------------------------------------------
    case_number = ensure_case_number(record)
    if not case_number:
        print("No case number provided. Leaving record in pending.")
        return False
    _setattr(record, "case_number", case_number)

    # ------------------------------------------------------------------
    # 2) Attempt DB fetch first
    # ------------------------------------------------------------------
    db_rec = None
    try:
        db_rec = find_case(session, case_number=case_number)
    except Exception as e:
        print(f"[WARN] DB lookup failed for case_number={case_number}: {e}")
        db_rec = None

    # ------------------------------------------------------------------
    # 3) Existing DB record flow
    # ------------------------------------------------------------------
    if db_rec is not None:
        _setattr(record, "has_existing_record", True)

        print("\nExisting DB record found.")
        db_case_like = db_record_to_case_record(db_rec)

        diffs = compare_records(db_case_like, record)
        if diffs:
            show_side_by_side(db_case_like, record, diffs)
        else:
            print("OCR record matches DB (no differences).")

        print("\nChoose action:")
        print("1) Keep DB (mark processed; discard OCR)")
        print("2) Discard OCR and keep it pending (come back later)")
        print("3) Edit OCR, then REPLACE DB entirely")
        print("4) Edit OCR, then MERGE selected fields into DB")
        print("5) MERGE selected fields into DB (no OCR editing)")

        choice = prompt_choice("Select option: ", ["1", "2", "3", "4", "5"])

        if choice == "1":
            return True

        if choice == "2":
            return False

        if choice in ("3", "4"):
            outcome = edit_record_loop(record)
            if outcome in ("discard", "skip"):
                return False

        # Enforce required fields before any write
        if not require_valid_or_offer_edit(record):
            return False

        # 3) Replace DB entirely
        if choice == "3":
            if not prompt_yes_no("REPLACE the DB record with this OCR record?", default=False):
                print("Cancelled. Leaving pending.")
                return False
            try:
                apply_replace(session, record)
                print("DB record replaced.")
                return True
            except Exception as e:
                session.rollback()
                print(f"[ERROR] Replace failed: {e}")
                return False

        # 4/5) Merge selected fields into DB
        if not diffs:
            print("No differences to merge. Marking processed.")
            return True

        fields_to_merge = choose_fields_to_merge(diffs)
        if not fields_to_merge:
            print("No fields selected; leaving pending.")
            return False

        updates: Dict[str, Any] = {}
        for f in fields_to_merge:
            val = _getattr(record, f)
            if isinstance(val, (date, datetime)):
                updates[f] = val
            else:
                updates[f] = _safe_enum_value(val)

        print("\nSelected updates:")
        for k, v in updates.items():
            before_val = _getattr(db_case_like, k)
            print(f" - {k}: {before_val}  ->  {updates[k]}")


        if not prompt_yes_no("Apply these updates to the DB?", default=False):
            print("Cancelled. Leaving pending.")
            return False

        try:
            apply_merge(session, case_number, updates)
            print(f"Merged {len(updates)} fields into DB.")
            return True
        except Exception as e:
            session.rollback()
            print(f"[ERROR] Merge failed: {e}")
            return False

    # ------------------------------------------------------------------
    # 4) No existing DB record -> insert flow
    # ------------------------------------------------------------------
    _setattr(record, "has_existing_record", False)

    print("\nNo existing DB record found for this case number.")
    pretty_print_record(record)

    print("\nChoose action:")
    print("1) Edit record, then INSERT")
    print("2) INSERT as-is (only if required fields are present)")
    print("3) Discard / leave in pending")

    choice = prompt_choice("Select option: ", ["1", "2", "3"])

    if choice == "3":
        return False

    if choice == "1":
        outcome = edit_record_loop(record)
        if outcome in ("discard", "skip"):
            return False

    # For insert-as-is, still must have required fields
    if not require_valid_or_offer_edit(record):
        return False

    if not prompt_yes_no("Insert this record into the database?", default=True):
        print("Cancelled. Leaving pending.")
        return False

    try:
        apply_new_insert(session, record)
        print("Inserted new record.")
        return True
    except Exception as e:
        session.rollback()
        print(f"[ERROR] Insert failed: {e}")
        return False

# =============================================================================
# PENDING REPAIR
# =============================================================================

def _pending_items(pending: Any) -> List[Tuple[str, Any]]:
    """
    Support pending as dict (key->CaseRecord) or list.
    Returns list of (key, record).
    """
    if isinstance(pending, dict):
        return list(pending.items())
    if isinstance(pending, list):
        out = []
        for i, rec in enumerate(pending):
            key = _getattr(rec, "pdf_name") or _getattr(rec, "pdfName") or str(i)
            out.append((key, rec))
        return out
    return []


def _remove_pending_by_key(pending: Any, key: str) -> Any:
    if isinstance(pending, dict):
        pending.pop(key, None)
        return pending
    if isinstance(pending, list):
        # remove first matching pdf_name-ish
        new_list = []
        for rec in pending:
            rec_key = _getattr(rec, "pdf_name") or _getattr(rec, "pdfName")
            if rec_key == key:
                continue
            new_list.append(rec)
        return new_list
    return pending


def repair_pending_cases(session: Any) -> None:
    pending = load_pending()
    items = _pending_items(pending)
    if not items:
        print("No pending records.")
        return

    processed_keys: List[str] = []

    for key, rec in items:
        print("\n" + "=" * 70)
        print(f"Repairing pending item: {key}")

        # Run edit/validate/commit loop
        processed = validate_and_commit_flow(session, rec)

        if processed:
            processed_keys.append(key)
            pending = _remove_pending_by_key(pending, key)
            write_pending(pending)
            print(f"Removed '{key}' from pending.")
        else:
            # Keep pending as-is
            write_pending(pending)
            print(f"Left '{key}' in pending.")
            
        if not prompt_yes_no("Would you like to continue to work on cases?", default=True):
            return

    print(f"\nDone. Processed {len(processed_keys)} pending records.")


# =============================================================================
# SUSPICIOUS DB REPAIR
# =============================================================================

def _fetch_suspicious(session: Any) -> List[Any]:
    """
    Try a few strategies depending on your queries.py.
    """
    if find_quality_candidates:
        return list(find_quality_candidates(session))
    else:
        return []

def quality_issues(case: CaseRecord) -> list[str]:
    issues = []

    # Case number
    if not re.fullmatch(r"\d{2,4}-\d{4,6}", case.case_number):
        issues.append("case_number format looks wrong")

    # Victim
    if case.victim is None:
        issues.append("victim missing")
    elif re.search(r"[\"'$\|²³¹ⁿᵈ]", case.victim):
        issues.append("victim contains weird characters")

    # Age
    if case.age is None:
        issues.append("age missing")
    elif case.age < 0 or case.age >= 100:
        issues.append(f"age unrealistic ({case.age})")

    # Sex
    if case.sex == Sex.NA:
        issues.append("sex defaulted to N/A")

    # Race
    if case.race == Race.OTHER:
        issues.append("race defaulted to Other")

    # Location
    if re.search(r"[\"'$\|²³¹ⁿᵈ°™]", case.location):
        issues.append("location contains OCR artifacts")

    # Synopsis
    if case.synopsis is None:
        issues.append("synopsis missing")
    elif re.search(r"[\"'$\|²³¹ⁿᵈ°™]", case.synopsis):
        issues.append("synopsis contains weird characters")

    return issues

def repair_suspicious_cases(session: Any) -> None:
    cases = _fetch_suspicious(session)
    if not cases:
        print("No suspicious DB records found.")
        return

    print(f"Found {len(cases)} suspicious DB records.")

    for db_rec in tqdm(cases, desc="Suspicious Records: "):
        issues = quality_issues(db_rec)
        
        if not issues:
            continue # false postive from SQL pass
        
        print("\n" + "=" * 70)
        case_number = getattr(db_rec, "case_number", None)
        print(f"Suspicious DB record: {case_number}")
        for issue in issues:
                print(f" - {issue}")

        if not case_number:
            print("[WARN] Skipping: DB record has no case_number.")
            continue

        # Snapshot so we don't mutate ORM object directly
        editable = db_record_to_case_record(db_rec)

        outcome = edit_record_loop(editable)  # "done" | "discard" | "skip"
        if outcome != "done":
            print("No changes applied (discard/skip).")
            if not prompt_yes_no("Continue to next suspicious record?", default=True):
                return
            continue

        # Determine updates (only changed fields)
        updates: Dict[str, Any] = {}

        for f in EDITABLE_FIELDS.keys():
            before = getattr(db_rec, f, None)
            after = getattr(editable, f, None)

            before_n = _safe_enum_value(before)
            after_n = _safe_enum_value(after)

            if isinstance(before_n, (date, datetime)):
                before_cmp = before_n.isoformat()
            else:
                before_cmp = before_n

            if isinstance(after_n, (date, datetime)):
                after_cmp = after_n.isoformat()
            else:
                after_cmp = after_n

            if before_cmp != after_cmp:
                updates[f] = after_n


        if not updates:
            print("No changes.")
            if not prompt_yes_no("Continue to next suspicious record?", default=True):
                return
            continue

        print("\nProposed updates:")
        for k, v in updates.items():
            print(f" - {k}: {getattr(db_rec, k, None)}  ->  {v}")

        if not prompt_yes_no("Apply these changes to the database?", default=False):
            print("Cancelled.")
            if not prompt_yes_no("Continue to next suspicious record?", default=True):
                return
            continue

        try:
            apply_merge(session, case_number, updates)
            print("Updated DB record.")
        except Exception as e:
            session.rollback()
            print(f"[ERROR] Failed to update DB record {case_number}: {e}")

        if not prompt_yes_no("Would you like to continue processing cases?", default=True):
            return

# =============================================================================
# MAIN MENU
# =============================================================================

def _get_session() -> Any:
    if not SessionLocal:
        raise RuntimeError("SessionLocal not found. Check db.engine import in this file.")
    return SessionLocal()


def human_repair() -> None:
    session = _get_session()
    print("Database session established.")

    while True:
        print("\nRepair Menu")
        print("1. Repair pending (parse failures)")
        print("2. Repair suspicious database records")
        print("3. Exit")
        choice = prompt_choice("Select option: ", ["1", "2", "3"])

        if choice == "1":
            repair_pending_cases(session)
        elif choice == "2":
            repair_suspicious_cases(session)
        else:
            print("Goodbye.")
            break

    try:
        session.close()
    except Exception:
        pass


if __name__ == "__main__":
    human_repair()
