import requests
from bs4 import BeautifulSoup
import os
import time
from tqdm import tqdm
from db import SessionLocal
from ingest.queries import find_case
from ingest.pending import write_pending
from typing import Dict, Optional
from ingest.domain import PDF_DIR, CaseRecord, CaseStatus, Mode
from sqlalchemy.exc import MultipleResultsFound


# Rate limit variable
RATE_LIMIT_SECONDS = 1.0 
    
def find_existing_case_status(pdf_name: str, session) -> Optional[str]:
    try:
        year_str, rest = pdf_name.split("-", 1)
        year = int(year_str)
    except ValueError:
        return None

    if year < 30:
        year += 2000
    elif year <= 99:
        year += 1900
    else:
        return None

    last_name = rest.replace(".pdf", "").replace("-", " ")
    try:
        result = find_case(session=session, victim=last_name, year=year, mode=Mode.AND)
        return result.status if result else None
    except MultipleResultsFound:
        print(
            f"[FETCH WARNING] Multiple existing cases matched "
            f"(year={year}, last_name={last_name}). "
            f"Forcing update."
        )
        return None

    
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
                existing_status = find_existing_case_status(pdfName, session)
                exists = existing_status is not None
            
                # If I don't have the pdf or if the status of the case changed, add the link
                if("solved" in pdfUrl and (not exists or (exists and existing_status != "solved"))):
                    cases[pdfName] = CaseRecord(
                        url=pdfUrl,
                        pdf_name=pdfName,
                        status=CaseStatus.SOLVED
                    )
                    
                # If I don't have the pdf or if the status of the case changed, add the link
                elif("warrant" in pdfUrl and (not exists or (exists and existing_status != "warrant"))):
                    cases[pdfName] = CaseRecord(
                        url=pdfUrl,
                        pdf_name=pdfName,
                        status=CaseStatus.WARRANT
                    )
                # If I don't have the pdf add it
                elif(not exists):
                    cases[pdfName] = CaseRecord(
                        url=pdfUrl,
                        pdf_name=pdfName,
                        status=CaseStatus.COLD
                    )
        
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
        filepath = PDF_DIR / filename

        # Skip if already downloaded
        if filepath.exists():
            downloaded[name] = case
            continue

        try:
            r = requests.get(case.url, headers=headers, timeout=15)
            r.raise_for_status()

            # Safety check
            if "application/pdf" not in r.headers.get("Content-Type", ""):
                print(f"Skipping non-PDF response: {case.url}")
                continue

            with open(filepath, "wb") as f:
                f.write(r.content)

            downloaded[name] = case

            # Polite delay
            time.sleep(RATE_LIMIT_SECONDS)

        except requests.RequestException as e:
            print(f"Failed to download {case.url}")
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