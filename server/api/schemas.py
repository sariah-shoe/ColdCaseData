from datetime import date
from pydantic import BaseModel, ConfigDict
from enum import Enum

class CaseStatus(str, Enum):
    solved = "solved"
    warrant = "warrant"
    cold = "cold"

class Sex(str, Enum):
    M = "M"
    F = "F"
    U = "U"

class Race(str, Enum):
    Black = "Black"
    White = "White"
    Hispanic = "Hispanic"
    Asian = "Asian"
    Native = "Native"
    Other = "Other"


class ColdCaseSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    case_number: str
    victim: str | None = None
    age: int | None = None
    sex: Sex | None = None
    race: Race | None = None
    incident_date: date
    location: str
    status: str | None = None
    
class ColdCaseDetail(BaseModel):
    synopsis: str | None = None
    
class HealthResponse(BaseModel):
    status: str
    
class ReadyResponse(BaseModel):
    status: str
    db: str

class CountByYear(BaseModel):
    year: int
    count: int