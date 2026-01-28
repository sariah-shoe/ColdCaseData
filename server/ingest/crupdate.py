from sqlalchemy import update
from db.models import ColdCase
from ingest.domain.case_record import CaseRecord

def insert_new_case(session, case: CaseRecord):
    session.add(
        ColdCase(
            case_number=case.case_number,
            victim=case.victim,
            age=case.age,
            sex=case.sex.value if case.sex else None,
            race=case.race.value if case.race else None,
            incident_date=case.incident_date,
            location=case.location,
            synopsis=case.synopsis,
            status=case.status.value,
        )
    )

def replace_case(session, case: CaseRecord):
    stmt = (
        update(ColdCase)
        .where(ColdCase.case_number == case.case_number)
        .values(
            victim=case.victim,
            age=case.age,
            sex=case.sex.value if case.sex else None,
            race=case.race.value if case.race else None,
            incident_date=case.incident_date,
            location=case.location,
            synopsis=case.synopsis,
            status=case.status.value,
        )
    )
    session.execute(stmt)

def merge_case_fields(session, case_number: str, updates: dict):
    stmt = (
        update(ColdCase)
        .where(ColdCase.case_number == case_number)
        .values(**updates)
    )
    session.execute(stmt)
