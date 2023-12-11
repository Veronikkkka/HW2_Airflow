from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
from sqlalchemy import create_engine, Table, Column, MetaData, Integer, String
from sqlalchemy.sql import select
from datetime import datetime


from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

from easyocr import Reader

from peopledatalabs import PDLPY



import requests
from bs4 import BeautifulSoup

import logging

def get_images():
    url = Variable.get("URL")
    response = requests.get(url)
    response_1 = BeautifulSoup(response.content, 'html.parser')
    images = set(img['src'] for img in response_1.find_all('img') if 'src' in img.attrs)
    hook = PostgresHook(postgres_conn_id='postgres')    
    existing = {row[0] for row in hook.get_records("SELECT url_for_image FROM urls_im")}
    new = images - existing
    logging.info(images)

    for image in new:
        hook.run("INSERT INTO image_links (image_url, processed) VALUES (%s, false)", parameters=(image,))
        
def deduplicate():
    DATABASE_URL = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
    engine = create_engine(DATABASE_URL)

    metadata = MetaData()

    company_table = Table('companies', metadata, autoload_with=engine)
    offer_table = Table('image_links', metadata, autoload_with=engine)

    conn = engine.connect()

    existing_companies = conn.execute(select([company_table.c.company_url])).fetchall()
    urls = conn.execute(select([offer_table.c.image_url])).fetchall()

    # Deduplication logic
    for url in urls:
        if url in existing_companies:
            # URL is already in the companies table, so you can take any action here (e.g., logging, deleting, updating)
            logging.info(f"Duplicate URL found: {url}")
            # Example: Delete the duplicate entry from the image_links table
            conn.execute(offer_table.delete().where(text("image_url = :url")).params(url=url))
        else:
            # URL is not in the companies table, you may want to keep it or perform other actions
            pass

    conn.close()

def get_company_info():
    urls = ti.xcom_pull("ocr_images")
    client = PDLPY(api_key=Variable.get("API_KEY"))
    hook = PostgresHook(postgres_conn_id='postgres')

    for url in urls:
        params = {"website": url}
        try:
            json_response = client.company.enrichment(**params).json()
            name = json_response.get("name")
            official_name = json_response.get("official_name")
            size = json_response.get("size")
            founded = json_response.get("founded")

            if name and official_name and size and founded:
                hook.run(
                    "INSERT INTO companies (url, official_name, founded, size) VALUES (%s, %s, %s, %s, %s)",
                    parameters=(url, name, official_name, founded, size)
                )
                
        except Exception as exception:
            logging.info(f"Error processing URL {url}: {exception}")
            continue
        
def process():
    hook = PostgresHook(postgres_conn_id='postgres')
    result = hook.get_records("SELECT url_for_image FROM urls_im WHERE processed = false")
    urls = []

    for row in result:
        url = row[0]
        reader = Reader(['en'])
        try:
            ocr_results = reader.readtext(url)
            text = ' '.join([res[1] for res in ocr_results])
            words = text.split()

            for i in range(1, len(words)):
                if words[i] in ['com', 'ua', 'gov', 'org', 'net']: 
                    example = words[i-1] + '.' + words[i]
                    urls.append(example)

        except Exception as exception:
            logging.info(f"Error processing image {url}: {exception}")
            continue
        
        hook.run("UPDATE urls_im SET processed = true WHERE url_for_image = %s", parameters=(url,))
        
    return urls

with DAG('data_injection_dag', start_date=datetime(2023, 12, 11), schedule_interval="@daily", catchup=True) as dag:
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS urls_im (
            id SERIAL PRIMARY KEY,
            url_for_image TEXT NOT NULL
        );
        """
    )

    create_company_table = PostgresOperator(
        task_id='create_company_table',
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS companies (
            url VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,            
            official_name VARCHAR(255),
            founded INT,
            size VARCHAR(10)
        );
        """
    )

    get_task = PythonOperator(
        task_id='get_images',
        python_callable=get_images,
    )
    
    ocr_process = PythonOperator(
        task_id='ocr_images',
        python_callable=process,
    )
    
    company_info = PythonOperator(
        task_id='company_info',
        python_callable=get_company_info,
    )
    
    deduplication_task = PythonOperator(
        task_id='deduplicate_data',
        python_callable=deduplicate,
    )

    create_table >> create_company_table >> get_task >> ocr_process >> company_info >> deduplication_task
