# Copyright 2023 woker001@gmail.com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""League of Legends Challenger Information Query."""
import pendulum

from airflow.operators.python import PythonOperator
from airflow import DAG, AirflowException
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from utils import BaseFetchOperator, USERS_NUMBER, set_cache, redis_conn, \
    postgres_conn, response_check, \
    response_filter, InsertSQLExecuteQueryOperator


class FetchSummonerIdOperator(BaseFetchOperator):
    """
    An object that searches for League of Legends Challenger users and
    stores the summoner ID in the database.
    """

    def __init__(self, *args, use_parallelism=False, **kwargs):
        super().__init__(*args, use_parallelism=use_parallelism, **kwargs)


class FetchPuuidOperator(BaseFetchOperator):
    """
    Search your League of Legends Challenger user ID
    and store the puuid in the database.
    """

    def __init__(self, *args, use_parallelism=True, **kwargs):
        super().__init__(*args, use_parallelism=use_parallelism, **kwargs)


def puuid_query(select_cols: str, values_cols: str) -> str:
    """Query creation function to store puuid in database.

    :param select_cols: List of columns to be inserted created with parameters_query()
    :param values_cols: A list of values to insert created with parameters_query()
    :return: completed query
    """
    now = pendulum.now()
    now = now.strftime('%Y-%m-%d %H:%M:%S.%f')
    return f"""
    INSERT INTO summoners_puuid ({select_cols}) 
    VALUES ({values_cols})  
    ON CONFLICT ("puuid")
    DO UPDATE SET "name" = excluded.name, 
            "profileIconId" = excluded."profileIconId", 
            "summonerLevel" = excluded."summonerLevel", 
            "revisionDate" = excluded."revisionDate", 
            "regDate" = '{now}';"""


def id_query(select_cols: str, values_cols: str) -> str:
    """
    Query creation function to store summoner id in database.

    Args:
        select_cols (str): List of columns to be inserted created with parameters_query().
        values_cols (str): A list of values to insert created with parameters_query().

    Returns:
        str: Completed query.
    """
    return f"INSERT INTO summoners ({select_cols}) VALUES ({values_cols}) ON CONFLICT DO NOTHING;"


def create_cache_ranking_table() -> None:
    """
    Retrieve the latest date of challenger users by hour and store them in Redis.
    """
    sql = f"""
    SELECT distinct ON ("summonerId") "summonerId", 
            "summonerName", "leaguePoints" , CAST("regDate" AS varchar) as "regDate"
    FROM (SELECT "summonerId", 
                "summonerName", 
                "leaguePoints", 
                DATE_TRUNC('hour',"regDate") as "regDate" 
    FROM summoners
    ORDER BY "regDate" DESC LIMIT {USERS_NUMBER}) as a;"""
    key = "ranking_table"
    set_cache(sql, key)


def create_cache_puuid_table() -> None:
    """
    Retrieve the latest date of challenger users by hour and store them in Redis.
    """
    conn_postgres = None
    conn_redis = None

    try:
        sql = f"""SELECT distinct ON ("puuid") "puuid", "name"
              FROM (SELECT "puuid", "name", DATE_TRUNC('hour',"regDate") as "regDate" 
              FROM summoners_puuid
              ORDER BY "regDate" DESC LIMIT {USERS_NUMBER}) as a;"""
        conn_postgres = postgres_conn()
        cursor = conn_postgres.cursor()
        cursor.execute(sql)
        data = dict(cursor.fetchall())
        conn_redis = redis_conn(redis_db_number=2)
        conn_redis.flushdb()
        conn_redis.mset(data)

    except Exception as exc:
        conn_redis.close()
        conn_postgres.close()
        raise AirflowException(f'REDIS ERROR : {exc}') from exc
    finally:
        if conn_redis:
            conn_redis.close()
        if conn_postgres:
            conn_postgres.close()


# This DAG receives user information of 300 challengers from the Riot API.
#
# Notes on usage:
#
# Turn on all dags.
#
# This DAG runs every 24 hours.
# It doesn't matter if this DAG doesn't run on a schedule.
#
# fetch_summonerId_task : Get summoner ID from RiotAPI. (300 challengers.)
#
# insert_summonerId_task : Save the summoner ID to the database.
#
# select_summonerId_task : Summoner ID lookup
#
# ranking_table_caching : Latest Date Challenger Ranking Save to Redis
#
# fetch_puuid_task : Get puuid from Riot API
#
# insert_puuid_task : Save the puuid to the database.
#
# puuid_table_caching : Save the final puuid information to redis.


with DAG(
        dag_id="challenger_leagues_queue",
        schedule="0 15 * * *",
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        catchup=False,
        tags=["RIOT", "api", "data_collection"]
) as dag:
    fetch_summonerId_task = FetchSummonerIdOperator(
        task_id='fetch_summonerId_task',
        http_conn_id='http_id_challengerleagues_queue',
        method='GET',
        headers={"X-Riot-Token": Variable.get("riot_api_key")},
        response_check=response_check,
        response_filter=response_filter,
    )

    insert_summonerId_task = InsertSQLExecuteQueryOperator(
        task_id='insert_summonerId_task',
        conn_id='postgres_conn',
        sql=None,
        parameters=fetch_summonerId_task.output,
        query_type=id_query
    )

    select_summonerId_task = SQLExecuteQueryOperator(
        task_id='select_summonerId_task',
        conn_id='postgres_conn',
        sql=f'SELECT "summonerId" FROM summoners ORDER BY "regDate" DESC LIMIT {USERS_NUMBER}',
    )

    ranking_table_caching = PythonOperator(
        task_id='champion_table_caching',
        python_callable=create_cache_ranking_table,
    )

    fetch_puuid_task = FetchPuuidOperator(
        task_id='fetch_puuid_task',
        http_conn_id='http_id_challengerleagues_puuid',
        method='GET',
        data=select_summonerId_task.output,
        headers={"api_key": Variable.get("riot_api_key")},
        response_check=response_check,
    )

    insert_puuid_task = InsertSQLExecuteQueryOperator(
        task_id='insert_puuid_task',
        conn_id='postgres_conn',
        autocommit=True,
        sql=None,
        parameters=fetch_puuid_task.output,
        query_type=puuid_query
    )

    puuid_table_caching = PythonOperator(
        task_id='puuid_table_caching',
        python_callable=create_cache_puuid_table,
    )

    fetch_summonerId_task >> insert_summonerId_task >> [ranking_table_caching, select_summonerId_task]  # pylint: disable=line-too-long pointless-statement
    select_summonerId_task >> fetch_puuid_task >> insert_puuid_task >> puuid_table_caching  # pylint: disable=line-too-long pointless-statement
