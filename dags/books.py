# dag - directed acyclic graph

#tasks : 1) fetch amazon data (extract), 2) clean data (transform), 3) load data in table to postgres (load)
#operators : Python Operator, Postgres Operator
#hooks - allows connection to postgres
#dependencies

from airflow import DAG
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 1) Extract: Function to fetch book data from Amazon + 2) Transform: Function to clean and transform the data

base_url = "https://www.amazon.com/s?k=data+engineering+books"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
                  " Chrome/90.0.4430.85 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

def scrape_books(ti, max_books=10):
    pg_hook = PostgresHook(postgres_conn_id='books_connection')
    
    # Get last state
    result = pg_hook.get_first("SELECT last_page, last_offset FROM amazon_scraper_state ORDER BY id DESC LIMIT 1;")
    page = result[0]
    offset = result[1]
    
    books = []
    visited_titles = set()

    while len(books) < max_books:
        url = f"{base_url}&page={page}"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        results = soup.find_all('div', {'data-component-type': 's-search-result'})
        if not results:
            break

        for idx, item in enumerate(results):
            if idx < offset:
                continue  # skip previously processed items

            if len(books) >= max_books:
                break

            title_tag = item.h2
            title = title_tag.text.strip() if title_tag else 'No title'
            if title in visited_titles:
                continue
            visited_titles.add(title)

            author_tag = item.find('a', class_='a-size-base a-link-normal s-underline-text s-underline-link-text s-link-style')
            author = author_tag.text.strip() if author_tag else 'Unknown author'

            price_whole = item.find('span', class_='a-price-whole')
            price_fraction = item.find('span', class_='a-price-fraction')
            if price_whole and price_fraction:
                price = f"${price_whole.text}{price_fraction.text}"
            else:
                price = 'Price not available'

            books.append({'title': title, 'author': author, 'price': price})
            offset = idx + 1  # update offset

        if len(books) < max_books:
            page += 1
            offset = 0  # reset offset for next page

    ti.xcom_push(key='books', value=books)
    ti.xcom_push(key='scraper_state', value={'page': page, 'offset': offset})

#3) Load: Function to load the data into PostgreSQL
def load_to_postgres(ti):
    books = ti.xcom_pull(key='books', task_ids='scrape_books')
    pg_hook = PostgresHook(postgres_conn_id='books_connection')  # Use your Airflow connection ID
    
    insert_query = """
    INSERT INTO data_engineering_books (title, author, price)
    VALUES (%s, %s, %s)
    ON CONFLICT DO NOTHING;
    """
    
    for book in books:
        pg_hook.run(insert_query, parameters=(book['title'], book['author'], book['price']))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='fetch_and_store_amazon_books',
    default_args=default_args,
    description='A simple DAG to fetch and load book data in Postgres',
    schedule=timedelta(days=1)
)

#operators : Python Operator, Postgres Operator
#hooks - allows connection to postgres

scrape_task = PythonOperator(
    task_id='scrape_books',
    python_callable=scrape_books,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_books_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

def init_state_table():
    pg_hook = PostgresHook(postgres_conn_id='books_connection')
    pg_hook.run("""
        CREATE TABLE IF NOT EXISTS amazon_scraper_state (
            id SERIAL PRIMARY KEY,
            last_page INT DEFAULT 1,
            last_offset INT DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    pg_hook.run("INSERT INTO amazon_scraper_state (last_page, last_offset) SELECT 1, 0 WHERE NOT EXISTS (SELECT 1 FROM amazon_scraper_state);")


def create_table_func():
    pg_hook = PostgresHook(postgres_conn_id='books_connection')
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS data_engineering_books (
        id SERIAL PRIMARY KEY,
        title TEXT,
        author TEXT,
        price TEXT
    );
    """
    pg_hook.run(create_table_sql)

def update_scraper_state(ti):
    state = ti.xcom_pull(task_ids='scrape_books', key='scraper_state')
    page = state['page']
    offset = state['offset']
    
    pg_hook = PostgresHook(postgres_conn_id='books_connection')
    pg_hook.run("UPDATE amazon_scraper_state SET last_page = %s, last_offset = %s, updated_at = NOW();", parameters=(page, offset))

# could not get PostgresOperator to work, so using PythonOperator instead

create_table = PythonOperator(
    task_id='create_table',
    python_callable=create_table_func,
    dag=dag,
)

update_state = PythonOperator(
    task_id='update_scraper_state',
    python_callable=update_scraper_state,
    dag=dag,
)

def remove_duplicates_func():
    pg_hook = PostgresHook(postgres_conn_id='books_connection')
    delete_duplicates_sql = """
    DELETE FROM data_engineering_books a
    USING data_engineering_books b
    WHERE
        a.id > b.id
        AND a.title = b.title
        AND a.author = b.author
        AND a.price = b.price;
    """
    pg_hook.run(delete_duplicates_sql)

remove_duplicates_task = PythonOperator(
    task_id='remove_duplicates',
    python_callable=remove_duplicates_func,
    dag=dag,
)

init_state = PythonOperator(
    task_id='init_state_table',
    python_callable=init_state_table,
    dag=dag,
)

# Then set dependencies
init_state >> create_table >> scrape_task >> load_task >> remove_duplicates_task >> update_state