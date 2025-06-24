from sqlalchemy import Column, Integer, String, Text, DateTime, Index
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class Filing(Base):
    __tablename__ = 'filings'

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False)
    filing_date = Column(String, nullable=False)     # or Date if you want stricter typing
    filing_type = Column(String, nullable=False)
    filing_url = Column(Text, nullable=True)
    document_url = Column(Text, nullable=False)
    source = Column(String, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.utcnow(), nullable=False) 

    # Add index on `symbol`
    __table_args__ = (
        Index('ix_symbol', 'symbol'),
    )
