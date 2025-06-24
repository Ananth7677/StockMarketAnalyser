from fastapi import logger
from sqlalchemy.ext.asyncio import AsyncSession
from DBO.DatabaseModels.filings_model import Filing
from sqlalchemy.future import select

async def insert_filing(db_session: AsyncSession, filing: Filing):
    try:
        db_session.add(filing)
            
        await db_session.commit()
        await db_session.refresh(filing)
    except Exception as e:
        logger.logger.error(f"Error inserting filing: {e}")
        await db_session.rollback()
        raise

async def get_filing_by_type_and_symbol(db_session: AsyncSession, filing_type, symbol) -> Filing :
    try:
        query = select(Filing).where( Filing.symbol == symbol , Filing.filing_type == filing_type)
        result = await db_session.execute(query)
        filing = result.scalars().first()
        result.close()
        return filing
    except Exception as e:
        logger.logger.error(f"Error retrieving filings: {e}")
        raise