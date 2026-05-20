from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id = 'booking_elt_pipeline',
    start_date = datetime(2026, 5, 1),
    schedule = '@daily',
    catchup = False,
    tags = ['booking', 'elt']    
)

def booking_elt_first_pipeline():
    @task()
    def extract():
        import json
        import os
        data_dir = '/Users/pujaverma/Desktop/BookingSolution/data'
        all_bookings = []
        for filename in os.listdir(data_dir):
            if filename.endswith('.json'):
                filepath = os.path.join(data_dir, filename)
                with open(filepath, 'r') as f:
                    data = json.load(f)
                    all_bookings.extend(data['bookings'])

        print(f'extracted{len(all_bookings)} bookings')
        return all_bookings
    
    @task()
    def load(bookings :list):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook 
        
        hook = SnowflakeHook(snowflake_conn_id = 'snowflake_default')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE booking_obj.raw.bookings")
        for b in bookings:
            cursor.execute('''
             insert into booking_obj.raw.bookings (booking_id,type,location,name,category,rating,price)
             values (%s,%s,%s,%s,%s,%s,%s) ''',
             (
                b['booking_id'],
                b['type'],
                b['location'],
                b['metadata']['name'],
                b['metadata'].get('category', None),
                b['metadata']['rating'],
                b['metadata']['price']
            ))
        cursor.close()
        conn.close()
        print(f'hello the bookings are: {len(bookings)} booking loaded into snowflake')

    raw = extract()
    load(raw)

booking_elt_first_pipeline()