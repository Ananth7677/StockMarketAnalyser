from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
import urllib.parse
import DBO.database_variables as db_base_vars

params = urllib.parse.quote_plus(
    f"DRIVER={db_base_vars.driver};"
    f"SERVER={db_base_vars.server};"
    f"DATABASE={db_base_vars.database};"
    f"UID={db_base_vars.username};"
    f"PWD={db_base_vars.password};"
    f"TrustServerCertificate=yes;"
)

DATABASE_URL = f"mssql+aioodbc:///?odbc_connect={params}"

engine = create_async_engine(DATABASE_URL, echo=False)

AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def get_db_session():
    async with AsyncSessionLocal() as session:
        yield session