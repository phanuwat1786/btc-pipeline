from airflow.sdk import DAG, task, Asset, Metadata, get_current_context, Variable
from airflow.providers.http.hooks.http import HttpHook
import pendulum
import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

btc_price_asset = Asset(name = 'btc_price',uri = 'x-market-price://btc')

with DAG(
    dag_id = 'get_btc_price',
    start_date = pendulum.parse('2026-05-03',tz = 'Asia/Bangkok'),
    schedule = '@hourly',
) as dag :
    
    logger = logging.getLogger(__name__)

    @task()
    def get_btc_price_from_api():
        
        http_hook = HttpHook(method= 'GET', http_conn_id= 'coindesk-api')
        header = {"Content-type":"application/json; charset=UTF-8"}

        response = http_hook.run(
            headers= header,
            data = {
                "fsym":"BTC","tsyms":"USD,THB","api_key": Variable.get(key = 'coindesk_api_key')
            },
            extra_options={'check_response': False})

        if response.ok:
            return response.json()
        else:
            response.raise_for_status()
    
    @task(
        outlets = [
            btc_price_asset
        ]
    )
    def save_btc_price(api_response:dict):
        df = pd.DataFrame([api_response])
        df['create_at_bi'] = pendulum.now().to_datetime_string()

        pg_hook = PostgresHook(
            postgres_conn_id = 'pg_market_price'
        )
        engine = pg_hook.get_sqlalchemy_engine()
        df.to_sql(name = 'bitcoin',con = engine, index = False, if_exists= 'append',schema = 'raw')

    t_get_btc_price_from_api = get_btc_price_from_api()
    t_save_btc_price = save_btc_price(api_response= t_get_btc_price_from_api)

    t_get_btc_price_from_api >> t_save_btc_price