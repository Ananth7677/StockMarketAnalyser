from datetime import datetime
import pickle

from bs4 import BeautifulSoup

from Agents.ScrapingAgent import scraping_base_variables
import Agents.ScrapingAgent.sec_edgar_filing_type_variables as filing_type_variables
import requests
import joblib
from pathlib import Path
import re
from sqlalchemy.ext.asyncio import AsyncSession
from Common.data_cleaning_operations import chunk_tables, chunk_text_by_sentence, embed_and_vector_store, validate_chunks
from DBO.DatabaseModels import filing_contents_model, filings_model
from DBO.Services.filing_contents_services import insert_filing_contents
import DBO.Services.filings_services as filing_services

# Cache directory (from your existing code)
CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)

async def get_report(company_name, filing_type:filing_type_variables.FilingType, db_session: AsyncSession):
    """
    Fetch the latest annual report for a given company from SEC EDGAR.
    
    Args:
        company_name: Stock ticker (e.g., "AAPL").
    
    Returns:
        List of dictionaries containing filing metadata and content.
    """
    # Todo add the cache_file_path here should dynamically take 10-K or 10-Q 
    cache_file = cache_file_path(company_name, filing_type)
    cached_result = check_cache_file_exists(cache_file)
    if cached_result:
        return cached_result

    try:
        search_url = scraping_base_variables.sec_edgar_complete_search_url.format(
            entity_name=company_name,
            # required_form_period=filing_type_variables.annual_report
            required_form_period = filing_type
        )
        print(search_url)
        response = requests.get(search_url, headers=scraping_base_variables.HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        filings = []
        rows = soup.select("tr")

        for row in rows:
            cells = row.find_all("td")
            if len(cells) >= 4:
                form_type = cells[0].text.strip()
                filing_date = cells[3].text.strip()
                link_tag = cells[1].find("a", href=True)
                if link_tag and form_type == filing_type:
                    filings.append({
                        "href": link_tag["href"],
                        "date": filing_date
                    })

        if not filings:
            raise ValueError(f"No filing type = {filing_type} filings found for the company{company_name}.")

        # Get the latest 10-K by filing date
        for f in filings:
            f["date_obj"] = datetime.strptime(f["date"], "%Y-%m-%d")

        latest_filing = max(filings, key=lambda x: x["date_obj"])
        required_url = f"{scraping_base_variables.sec_edgar_base_url}{latest_filing['href']}"

        # Fetch detailed filing page (filing documents list)
        new_request = requests.get(required_url, headers=scraping_base_variables.HEADERS)
        new_request.raise_for_status()
        soup_01 = BeautifulSoup(new_request.text, "html.parser")

        # Find the table with the filing documents
        doc_table = soup_01.find("table", class_="tableFile", summary="Document Format Files")
        if not doc_table:
            raise ValueError("Could not find document table in filing detail page.")

        main_doc_href = None
        rows = doc_table.find_all("tr")
        for row in rows[1:]:  # Skip header
            cells = row.find_all("td")
            if len(cells) >= 4:
                doc_type = cells[3].text.strip()
                doc_link_tag = cells[2].find("a", href=True)
                description = cells[1].text.strip().lower()
                if doc_link_tag and (doc_type == filing_type or "complete submission" in description) and doc_link_tag['href'].endswith((".htm", ".html")):
                    main_doc_href = doc_link_tag['href']
                    break

        if not main_doc_href:
            raise ValueError("Could not find HTML document link.")
        cleaned_url = clean_ixbrl_url(main_doc_href)
        main_doc_url = f"{scraping_base_variables.sec_edgar_base_url}{cleaned_url}"
        
        # Fetch the actual 10-K document content
        doc_response = requests.get(main_doc_url, headers=scraping_base_variables.HEADERS)
        doc_response.raise_for_status()
        soup_doc = BeautifulSoup(doc_response.text, "html.parser")
        print(f"Fetching document from: {main_doc_url}")
        # Extract the full text or specific tags
        tables = soup_doc.find_all('table')
        full_text = clean_content(soup_doc.text)
        for table in tables:
            full_text = full_text.replace(table.text, "")
            
        text_chunks = chunk_text_by_sentence(full_text, soup_doc, max_tokens=500, overlap_sentences=50)
        table_chunks = chunk_tables(tables, max_tokens=500)
        all_chunks = validate_chunks(text_chunks, table_chunks, max_tokens=500)
        
        vector_store = embed_and_vector_store(all_chunks)
        if vector_store:
            joblib.dump(vector_store, cache_file)
            print(f"Vector store saved to {cache_file}")
        filing_table_id = await create_filing_and_get_id(db_session, company_name, required_url, main_doc_url, latest_filing["date"], filing_type)
        await insert_chunks_into_filing_contents(db_session, all_chunks, company_name, filing_table_id)
        result = {
            "company": company_name,
            "filing_date": latest_filing["date"],
            "filing_url": required_url,
            "chunk_length": len(all_chunks[0]),
            "no of chunks": len(all_chunks),
            "document_url": main_doc_url,
            "content": all_chunks  # Full HTML content of the 10-K document
        }

        
        return [result]

    except Exception as e:
        print(f"Error fetching 10-K for {company_name}: {e}")
        return []
    

def cache_file_path(company_name:str, filing_type:filing_type_variables):
    return CACHE_DIR / f"{company_name}_{filing_type}_cache.pkl"
    
def check_cache_file_exists(cache_file):
    if cache_file.exists():
        with cache_file.open("rb") as f:
            print(f"Loading from cache: {cache_file}")
            return pickle.load(f)
    return None

def clean_ixbrl_url(viewer_url):
    """
    Converts an SEC iXBRL viewer URL (with ix?doc=...) to the actual document URL.
    
    Args:
        viewer_url (str): The viewer URL from SEC (contains 'ix?doc=').
    
    Returns:
        str: The actual document URL.
    """
    if 'ix?doc=' in viewer_url:
        return viewer_url.split('ix?doc=')[1]
    return viewer_url

def clean_content(html: str) -> str:
    """
    Remove XBRL tags, SEC-specific metadata, and noise from 10-K text.
    
    Args:
        html (str): Raw text from the 10-K document.
    
    Returns:
        str: Cleaned, readable text.
    """
    # Remove XBRL tags, SEC metadata, and other noise
    patterns = [
        r"\b(aapl-\d+|iso4217:[A-Z]+|xbrli:[a-z]+|us-gaap:[^\s]+|http://[^\s]+)\b",  # XBRL and URLs
        r"\b\d{8}\b",  # CIK-like numbers (e.g., 0000320193)
        r"\b[P][1-3][Y]\b",  # Period tags (e.g., P1Y)
        r"\b(FY|true|false)\b",  # SEC flags
        r"\b[a-z]+:[\w]+Member\b",  # Member tags (e.g., us-gaap:CommonStockMember)
        r"\b\d{5}\b",  # Zip codes (e.g., 95014)
        r"\(\d{3}\)\s*\d{3}-\d{4}",  # Phone numbers (e.g., (408) 996-1010)
        r"\$[0-9,.]+",  # Monetary values
        r"\s{2,}",  # Multiple spaces
    ]
    cleaned = html
    for pattern in patterns:
        cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)

    # Parse with BeautifulSoup to remove HTML/XML tags
    soup = BeautifulSoup(cleaned, "lxml")
    text = soup.get_text(separator= ' ', strip=True)

    # Remove extra whitespace and filter short/noisy segments
    text = " ".join(text.split())
    segments = [seg for seg in text.split(". ") if len(seg.split()) > 5 and not seg.replace(" ", "").isdigit()]
    print(segments)
    return ". ".join(segments)

async def create_filing_and_get_id(
    db_session: AsyncSession,
    company_name: str,
    required_url: str,
    main_doc_url: str,
    filing_date: str,
    filing_type: str
)->int:
    filing = initialize_filing(company_name, required_url, main_doc_url, filing_date, filing_type)
    await filing_services.insert_filing(db_session, filing)
    record = await filing_services.get_filing_by_type_and_symbol(db_session ,filing_type, company_name)
    return record.id

async def insert_chunks_into_filing_contents(db_session: AsyncSession, chunks:list[str], company_name: str, filing_table_id):
    i = 0
    for sentence_chunk in chunks:
        print(f"chunk number = {i}, length of chunk = {len(sentence_chunk)}")
        i = i+1
        filing_contents_var = initialize_filing_contents(company_name, sentence_chunk, filing_table_id)
        await insert_filing_contents(db_session, filing_contents_var)
        

def initialize_filing(company_name, required_url, main_doc_url, filing_date, filing_type):
    new_filing = filings_model.Filing()
    new_filing.symbol = company_name
    new_filing.filing_url = required_url
    new_filing.document_url = main_doc_url    
    new_filing.source = 'SEC'
    new_filing.filing_date = datetime.strptime(filing_date, "%Y-%m-%d")
    new_filing.filing_date = filing_date
    new_filing.filing_type = filing_type

    return new_filing

def initialize_filing_contents(company_name: str, content, filing_table_id):
    filing_contents_var = filing_contents_model.FilingContents()
    filing_contents_var.filing_table_id = filing_table_id
    filing_contents_var.symbol = company_name
    filing_contents_var.content = content
    filing_contents_var.created_by = 'Ananth'
    
    return filing_contents_var