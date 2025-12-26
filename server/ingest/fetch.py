import requests
from bs4 import BeautifulSoup
import sys
import os
import time
import json
from tqdm import tqdm
from sqlalchemy import select, extract
from db import SessionLocal
from db.models import ColdCase
from ingest.pending import write_pending
from typing import Dict, TypedDict, Tuple, Optional

class CaseRecord(TypedDict):
    url: str
    name: str
    source_status: str

# Global variables for downloading PDFs
PDF_DIR = "coldCasePDFs"
RATE_LIMIT_SECONDS = 1.0 
    
def findRecord(pdfName: str, session) -> Tuple[bool, str]:
    # Split the name so that I can get the year and the last name
    yearName = pdfName.split("-")
    
    # Format the year
    try:
        year = int(yearName[0])
    except ValueError:
        return False, "N/A"

    if year < 70:
        year = 2000 + year if year < 10 else 2000 + year
    elif 70 <= year <= 99:
        year = 1900 + year
    else:
        return False, "N/A"

    # Extract the last name
    lastName = yearName[1].replace(".pdf", "")
    
    # Try to find the record of this person
    stmt = (
        select(ColdCase.status)
        .where(ColdCase.victim.ilike(f"%{lastName}%"))
        .where(extract("year", ColdCase.incident_date) == year)
    )

    result = session.execute(stmt).scalar_one_or_none()
    
    # Return whether or not they are found and the status of their case
    if(result is None):
        return(False, "N/A")
    else:
        return(True, result)
    
    
def getURLs(session) -> Dict[str, CaseRecord]:
    # URL and header for request
    url = "https://www.denvergov.org/Government/Agencies-Departments-Offices/Agencies-Departments-Offices-Directory/Police-Department/Crime-Information/Cold-Cases"
    headers = {"User-Agent": "Cold Case Research Bot (sariahshoe@gmail.com)"}

    try: 
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
    except requests.RequestException as e:
        print(e)
        return {}

    # Only parse the data if a successful response is given
    if r.status_code == 200:
        # Lists to hold my links
        cases = {}

        # Soup my html
        soup = BeautifulSoup(r.text, "html.parser")

        # Pull the links for cases
        allLinks = soup.find_all( "a",
            class_=["document", "ext-pdf", "opens-in-new-tab"]
        )

        if not allLinks:
            print("No PDF links found. Please check the class name or page structure.")
            return (cases)
        
        # Go through each link
        for link in tqdm(allLinks, desc="Scanning case links"):
            # Format the link
            pdfUrl = f"https://www.denvergov.org{link['href']}"
            
            # Path to PDF looks like this "/files/assets/public/v/1/police-department/documents/cold-cases/warrant/year-lastName.pdf"
            # I want the information from the end of the path so I can check if I have the person in my database
            pdfName = pdfUrl.split("/")[-1]
            
            # Find out if I already have the pdf 
            if(pdfUrl.lower().endswith(".pdf")):
                exists, status = findRecord(pdfName, session)
            
                # If I don't have the pdf or if the status of the case changed, add the link
                if("solved" in pdfUrl and (not exists or (exists and status != "solved"))):
                    cases[pdfName] = {
                        "url" : pdfUrl,
                        "name" : pdfName,
                        "source_status" : "solved"
                    }
                    
                # If I don't have the pdf or if the status of the case changed, add the link
                elif("warrant" in pdfUrl and (not exists or (exists and status != "warrant"))):
                    cases[pdfName] = {
                        "url" : pdfUrl,
                        "name" : pdfName,
                        "source_status" : "warrant"
                    }
                # If I don't have the pdf add it
                elif(not exists):
                    cases[pdfName] = {
                        "url" : pdfUrl,
                        "name" : pdfName,
                        "source_status" : "cold"
                    }
        
        # Return my lists that I need to pull
        return(cases)


def downloadPDFs(cases: Dict[str, CaseRecord]) -> Dict[str, CaseRecord]:
    # Header for politeness
    headers = {
        "User-Agent": "Cold Case Research Bot (sariahshoe@gmail.com)"
    }

    # Make sure I have my directory for PDFs
    os.makedirs(PDF_DIR, exist_ok=True)

    # Keep track of whats downloaded
    downloaded = {}

    # Go through each case
    for name, case in tqdm(cases.items(), desc="Downloading PDFs"):
        filename = name
        filepath = os.path.join(PDF_DIR, filename)

        # Skip if already downloaded
        if os.path.exists(filepath):
            downloaded[name] = case
            continue

        try:
            r = requests.get(case["url"], headers=headers, timeout=15)
            r.raise_for_status()

            # Safety check
            if "application/pdf" not in r.headers.get("Content-Type", ""):
                print(f"Skipping non-PDF response: {case['url']}")
                continue

            with open(filepath, "wb") as f:
                f.write(r.content)

            downloaded[name] = case

            # Polite delay
            time.sleep(RATE_LIMIT_SECONDS)

        except requests.RequestException as e:
            print(f"Failed to download {case['url']}")
            print(e)
            continue

    return downloaded

def pullAllData() -> None:
    session = SessionLocal()
    try:
        print("Database session established.")

        cases = getURLs(session)
        cases = downloadPDFs(cases)
        write_pending(cases)

    finally:
        session.close()