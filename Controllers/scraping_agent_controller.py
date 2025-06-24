from fastapi import APIRouter, Depends
import Agents.ScrapingAgent.scraping_base_functions as sbf
import Agents.ScrapingAgent.sec_edgar_filing_type_variables as filing_type_variables
from sqlalchemy.ext.asyncio import AsyncSession

from DBO.db_connection import get_db_session

router = APIRouter()

@router.get("/GetAnnualReport")
async def get_annual_report_of_company(
    company_name: str,
    filing_type: filing_type_variables.FilingType,
    db_session: AsyncSession = Depends(get_db_session)
):
    return await sbf.get_report(company_name, filing_type.value, db_session)

