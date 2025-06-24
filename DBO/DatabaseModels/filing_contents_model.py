from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timezone

Base = declarative_base()

class FilingContents(Base):
    __tablename__ = 'filing_contents'

    filing_contents_id = Column(Integer, primary_key=True, autoincrement=True)
    filing_table_id = Column(Integer, nullable= False)
    symbol = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    date_created = Column(DateTime, default=lambda: datetime.utcnow(), nullable=False)
    created_by = Column(String, nullable=False)

    # Add index on `symbol`
    __table_args__ = (
        Index('ix_filing_table_id', 'filing_table_id'),
        Index('ix_symbol', 'symbol')
    )