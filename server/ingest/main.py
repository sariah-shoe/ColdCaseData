# python -m ingest.main

from ingest.fetch import pullAllData
from ingest.ingest import parseAllPDFs
from ingest.repair import human_repair

def main():
    # pullAllData()
    # parseAllPDFs()
    human_repair()
    
if __name__ == "__main__":
    main()