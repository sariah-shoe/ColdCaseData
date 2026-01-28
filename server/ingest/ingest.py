from db import SessionLocal
from ingest.queries import find_case
from ingest.crupdate import insert_new_case
from ingest.parsing import parseOne
from ingest.pending import load_pending, remove_processed, write_pending
from tqdm import tqdm
    
def parseAllPDFs():
    session = SessionLocal()
    try:
        cases = load_pending()
        processed_keys = []

        for key, case in tqdm(cases.items(), desc="OCR scan"):
            parsed_case = parseOne(session, case)
            
            if not parsed_case:
                continue

            if parsed_case.has_required_fields():
                if( not find_case(session, case_number=parsed_case.case_number)):
                    insert_new_case(session, parsed_case)
                   
                parsed_case.has_existing_record = True
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
