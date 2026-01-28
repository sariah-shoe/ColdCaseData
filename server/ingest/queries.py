from typing import Optional, List
from sqlalchemy import select, extract
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, not_

from db.models import ColdCase
from ingest.domain.case_record import Sex, Race, CaseStatus, Mode
    
def combine_conditions(conditions: list, mode: Mode):
    if not conditions:
        return None

    if mode == Mode.AND:
        return and_(*conditions)

    if mode == Mode.OR:
        return or_(*conditions)

    if mode == Mode.EX_OR:
        if len(conditions) < 2:
            return conditions[0]
        return and_(
            or_(*conditions),
            not_(and_(*conditions))
        )

    raise ValueError(f"Unsupported mode: {mode}")

def find_case(
    session: Session,
    *,
    case_number: Optional[str] = None,
    sex: Optional[Sex] = None,
    race: Optional[Race] = None,
    status: Optional[CaseStatus] = None,
    victim: Optional[str] = None,
    age: Optional[int] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
    day: Optional[int] = None,
    location: Optional[str] = None,
    synopsis: Optional[str] = None,
    mode: Optional[Mode] = Mode.OR
) -> Optional[ColdCase]:
    """
    Find a single cold case matching the given criteria.
    Returns None if no match is found.
    """

    conditions = []
    
    if case_number is not None:
        conditions.append(ColdCase.case_number.ilike(f"%{case_number}%"))

    if victim is not None:
        conditions.append(ColdCase.victim.ilike(f"%{victim}%"))
        
    if age is not None:
        conditions.append(ColdCase.age == age)
        
    if sex is not None:
        conditions.append(ColdCase.sex == sex.value)

    if race is not None:
        conditions.append(ColdCase.race == race.value)
        
    if year is not None:
         conditions.append(extract("year", ColdCase.incident_date) == year)
         
    if month is not None:
         conditions.append(extract("month", ColdCase.incident_date) == month)
         
    if day is not None:
         conditions.append(extract("day", ColdCase.incident_date) == day)
        
    if location is not None:
        conditions.append(ColdCase.location.ilike(f"%{location}%"))
        
    if synopsis is not None:
        conditions.append(ColdCase.synopsis.ilike(f"%{synopsis}%"))

    if status is not None:
        conditions.append(ColdCase.status == status.value)

    combined = combine_conditions(conditions, mode)
    stmt = select(ColdCase)

    if combined is not None:
        stmt = stmt.where(combined)
        
    return session.execute(stmt).scalar_one_or_none()


def find_cases(
    session: Session,
    *,
    sex: Optional[Sex] = None,
    race: Optional[Race] = None,
    status: Optional[CaseStatus] = None,
    victim: Optional[str] = None,
    age: Optional[int] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
    day: Optional[int] = None,
    location: Optional[str] = None,
    synopsis: Optional[str] = None,
    mode: Optional[Mode] = Mode.OR
) -> List[ColdCase]:
    """
    Find multiple cold cases matching the given filters.
    Intended for repair / audit workflows.
    """

    conditions = []

    if victim is not None:
        conditions.append(ColdCase.victim.ilike(f"%{victim}%"))
        
    if age is not None:
        conditions.append(ColdCase.age == age)
        
    if sex is not None:
        conditions.append(ColdCase.sex == sex.value)

    if race is not None:
        conditions.append(ColdCase.race == race.value)
        
    if year is not None:
        conditions.append(extract("year", ColdCase.incident_date) == year)
         
    if month is not None:
        conditions.append(extract("month", ColdCase.incident_date) == month)
         
    if day is not None:
        conditions.append(extract("day", ColdCase.incident_date) == day)
        
    if location is not None:
        conditions.append(ColdCase.location.ilike(f"%{location}%"))
        
    if synopsis is not None:
        conditions.append(ColdCase.synopsis.ilike(f"%{synopsis}%"))

    if status is not None:
        conditions.append(ColdCase.status == status.value)
        
    combined = combine_conditions(conditions, mode)
    stmt = select(ColdCase)

    if combined is not None:
        stmt = stmt.where(combined)

    return session.execute(stmt).scalars().all()

WEIRD_CHAR_REGEX = r"[\"'$\|²³¹ⁿᵈ°™]"

def find_quality_candidates(session: Session):
    stmt = select(ColdCase).where(
        or_(
            # Sex & race defaults
            ColdCase.sex == "N/A",
            ColdCase.race == "Other",

            # Optional fields missing
            ColdCase.victim.is_(None),
            ColdCase.age.is_(None),
            ColdCase.synopsis.is_(None),

            # Weird characters
            ColdCase.victim.op("~*")(WEIRD_CHAR_REGEX),
            ColdCase.location.op("~*")(WEIRD_CHAR_REGEX),
            ColdCase.synopsis.op("~*")(WEIRD_CHAR_REGEX),

            # Case number looks suspicious (length / chars)
            ColdCase.case_number.op("~")(r"[^0-9\-]"),
            ColdCase.case_number.op("!~")(r"^\d{2,4}-\d+$"),
        )
    )

    return session.execute(stmt).scalars().all()
