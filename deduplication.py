from sqlalchemy import create_engine, Table, Column, MetaData, Integer, String
from sqlalchemy.sql import select

# Database connection
DATABASE_URL = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
engine = create_engine(DATABASE_URL)

metadata = MetaData()

# Define your company and offer tables
company_table = Table('company', metadata, Column('id', Integer, primary_key=True), Column('name', String))
offer_table = Table('offer', metadata, Column('id', Integer, primary_key=True), Column('company_id', Integer), Column('details', String))

def deduplicate():
    conn = engine.connect()

    existing_companies = conn.execute(select([company_table.c.name])).fetchall()
    new_data = [{'company': 'Company1', 'details': 'Offer1'}, {'company': 'Company2', 'details': 'Offer2'}, ...]

    for new_offer in new_data:
        company_name = new_offer['company']
        offer_details = new_offer['details']

        if (company_name,) in existing_companies:            
            update_stmt = offer_table.update().where(company_table.c.name == company_name).values(details=offer_details)
            conn.execute(update_stmt)
        else:
           
            insert_stmt = company_table.insert().values(name=company_name)
            conn.execute(insert_stmt)
            new_company_id = conn.execute(select([company_table.c.id]).where(company_table.c.name == company_name)).scalar()
            insert_stmt = offer_table.insert().values(company_id=new_company_id, details=offer_details)
            conn.execute(insert_stmt)

    conn.close()

if __name__ == "__main__":
    deduplicate()
