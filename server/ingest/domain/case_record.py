from dataclasses import dataclass, field
from typing import Optional, List
from datetime import date
from enum import Enum

class Mode(Enum):
    OR = "OR"
    AND = "AND"
    EX_OR = "EX_OR"

class Sex(Enum):
    M = "M"
    F = "F"
    NA = "N/A"


class Race(Enum):
    WHITE = "White"
    BLACK = "Black"
    HISPANIC = "Hispanic"
    ASIAN = "Asian"
    PACIFIC_ISLANDER = "Pacific Islander"
    NATIVE_AMERICAN = "Native American"
    OTHER = "Other"


class CaseStatus(Enum):
    SOLVED = "solved"
    WARRANT = "warrant"
    COLD = "cold"


@dataclass
class CaseRecord:
    url: str
    pdf_name: str
    status: CaseStatus

    case_number: Optional[str] = None
    victim: Optional[str] = None
    age: Optional[int] = None
    sex: Optional[Sex] = None
    race: Optional[Race] = None
    incident_date: Optional[date] = None
    location: Optional[str] = None
    synopsis: Optional[str] = None
    has_existing_record: bool = False
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "url": self.url,
            "pdf_name": self.pdf_name,
            "status": self.status.value if self.status else None,
            "case_number": self.case_number,
            "victim": self.victim,
            "age": self.age,
            "sex": self.sex.value if self.sex else None,
            "race": self.race.value if self.race else None,
            "incident_date": (
                self.incident_date.isoformat()
                if self.incident_date else None
            ),
            "location": self.location,
            "synopsis": self.synopsis,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CaseRecord":
        return cls(
            url=data["url"],
            pdf_name=data["pdf_name"],
            status=CaseStatus(data["status"]),
            case_number=data.get("case_number"),
            victim=data.get("victim"),
            age=data.get("age"),
            sex=Sex(data["sex"]) if data.get("sex") else None,
            race=Race(data["race"]) if data.get("race") else None,
            incident_date=(
                date.fromisoformat(data["incident_date"])
                if data.get("incident_date") else None
            ),
            location=data.get("location"),
            synopsis=data.get("synopsis"),
        )

    def has_required_fields(self) -> bool:
        return bool(self.case_number and self.incident_date and self.location)
    
    def __str__(self) -> str:
        def fmt(value):
            if value is None:
                return "—"
            if hasattr(value, "value"):  # Enum
                return value.value
            return value

        lines = [
            "=" * 72,
            f"PDF        : {self.pdf_name}",
            f"URL        : {self.url}",
            f"Status     : {fmt(self.status)}",
            f"Case #     : {fmt(self.case_number)}",
            "-" * 72,
            f"Victim     : {fmt(self.victim)}",
            f"Age        : {fmt(self.age)}",
            f"Sex        : {fmt(self.sex)}",
            f"Race       : {fmt(self.race)}",
            f"Date       : {fmt(self.incident_date)}",
            f"Location   : {fmt(self.location)}",
            "-" * 72,
            "Synopsis:",
            self.synopsis,
        ]

        if self.has_existing_record:
            lines.append("\nExisting database record found")

        if self.warnings:
            lines.append("\nWarnings:")
            for w in self.warnings:
                lines.append(f"  - {w}")

        lines.append("=" * 72)
        return "\n".join(lines)

        