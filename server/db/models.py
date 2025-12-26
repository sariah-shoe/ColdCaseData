from sqlalchemy import Column, Text, Integer, Date
from .base import Base

class ColdCase(Base):
    __tablename__ = "cold_cases"

    case_number = Column(Text, primary_key=True)

    victim = Column(Text)
    age = Column(Integer)

    sex = Column(Text)
    race = Column(Text)

    incident_date = Column(Date, nullable=False)
    location = Column(Text, nullable=False)

    synopsis = Column(Text)
    status = Column(Text)
