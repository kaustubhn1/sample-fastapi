from sqlalchemy import Column, Integer, Float, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, Float, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, Float, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import Column, Integer, String,Numeric,DateTime





Base = declarative_base()

username = "postgres"
password = "postgres"
host = "localhost"
database = "inventory"
engine = create_engine(
    f"postgresql://{username}:{password}@{host}/{database}", pool_pre_ping=True
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False)


class ItemBalanceModel(Base):
    __tablename__ = 'item_balance'

    id = Column(Integer, primary_key=True, index=True)
    item_id = Column(Integer)
    quantity = Column(Numeric)
    date_production_start = Column(DateTime)
    date_received_into_inventory = Column(DateTime)
    date_shipped_from_inventory = Column(DateTime)

    def __repr__(self):
        return f"<ItemBalanceModel(id={self.id}, item_id={self.item_id}, quantity={self.quantity}, date_production_start={self.date_production_start}, date_received_into_inventory={self.date_received_into_inventory}, date_shipped_from_inventory={self.date_shipped_from_inventory})>"
    
class InventoryData(Base):
    __tablename__ = "inventory_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    item_id = Column(Integer)
    quantity = Column(Float)
    date_production_start = Column(TIMESTAMP)
    date_received_into_inventory = Column(TIMESTAMP)
    date_shipped_from_inventory = Column(TIMESTAMP)
    transaction_timestamp = Column(TIMESTAMP)


def insert_into_postgres(data, table_name, engine):
    data.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False,
    )


def get_engine():
    return engine


# Dependency to get a database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
