from fastapi import logger
from sqlalchemy.ext.asyncio import AsyncSession
from DBO.DatabaseModels.filing_contents_model import FilingContents


async def insert_filing_contents(db_session: AsyncSession, filing_contents: FilingContents):
    try:
        db_session.add(filing_contents)
            
        await db_session.commit()
        await db_session.refresh(filing_contents)
        return filing_contents
    except Exception as e:
        logger.logger.error(f"Error inserting filing: {e}")
        await db_session.rollback()
        raise