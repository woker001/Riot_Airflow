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

"""League of Legends Challenger Match Information Query."""
import re
from typing import List, Dict, Any
import pandas as pd
import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.db import provide_session
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow import DAG, AirflowException
from airflow.models import Variable

from utils import BaseFetchOperator, ThreadPoolExecutor, \
    InsertSQLExecuteQueryOperator, set_cache, USERS_NUMBER, \
    get_cache, minmax_scaler, redis_conn, postgres_conn, response_check


class FetchSummonerMatchIdOperator(BaseFetchOperator):
    """
    Use the summoner puuid to look up the match id.
    Calls an endpoint on an HTTP system to execute an action.
    """

    def execute(self, context: Dict[str, Any]) -> List[str]:
        """
        This function executes request to http api.
        This function executes the request to the http api and receives the puuid as the ID
        of the user as the return value.

        Args:
            context (Dict[str, Any]): Contains information indicating
            the current DAG execution status in Airflow.
            For example, information about DAG execution,
            such as dag_id, task_id, and execution_date, is included in the context object.

        Returns:
            List[str]: Other information other than puuid searched by summoner ID.
        """

        results = self.execute_request_in_parallel(context)  # Get matchId
        results = list(set(results))

        # TODO : Need a way to eliminate duplicate matches.
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
        sql = pg_hook.get_records(sql='SELECT "matchId" FROM match_result')
        sql = [item[0] for item in sql]
        self.log.info([result for result in results if result in sql])
        results = [result for result in results if result not in sql]
        return results

    def execute_request_in_parallel(self, context: Dict[str, Any]) -> List[str]:
        """
        Parallel processing function.

        Args:
            context (Dict[str, Any]): Contains information indicating
            the current DAG execution status in Airflow.

        Returns:
            List[str]: Match ID retrieved by puuid (A list that is not a double list).
        """
        with ThreadPoolExecutor(max_workers=self.parallelism) as executor:
            futures = [executor.submit(self.run_request,
                                       self.puuid_endpoint(endpoint[0]),
                                       context
                                       ) for endpoint in self.data
                       ]
            results = [future.result() for future in futures]
        single_list_results = [item for sublist in results for item in sublist]
        return single_list_results

    def puuid_endpoint(self, endpoint: str) -> str:
        """
         Get the matchId endpoint based on puuid.

         Args:
             endpoint (str): puuid.

         Returns:
             str: matchId endpoint.
         """
        self.log.info(endpoint)
        return f'{endpoint}/ids?start=0&count=5'


class FetchMatchInformationOperator(BaseFetchOperator):
    """
    Search match information by match ID.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute_request_in_parallel(self, context):
        """
        :return: A list that is not a double list
        """
        with ThreadPoolExecutor(max_workers=self.parallelism) as executor:
            futures = [executor.submit(self.run_request,
                                       endpoint, context) for endpoint in self.data
                       ]
            results = [future.result() for future in futures]
        return results


class InsertSQLMatchResultOperator(InsertSQLExecuteQueryOperator):  # pylint: disable=too-many-ancestors
    """
    Match information retrieved by match ID is stored in the database.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.cols_list = ["matchId",
                          "participants",
                          "gameCreation",
                          "gameDuration",
                          "gameEndTimestamp",
                          "gameId",
                          "gameMode",
                          "gameName",
                          "gameStartTimestamp",
                          "gameType",
                          "gameVersion",
                          "mapId",
                          "platformId",
                          "queueId"]
        self.hook = self.get_db_hook()

    def execute(self, context: Dict[str, Any]) -> None:
        """
        Execute the SQL query to insert match information into the database.

        Args:
            context (Dict[str, Any]): Contains information indicating
            the current DAG execution status in Airflow.
        """
        self.log.info("Executing: %s", self.sql)
        select_cols, values_cols = self.parameters_query(self.cols_list,
                                                         is_col=True)
        self.sql = f"""INSERT INTO match_result ({select_cols})
                        VALUES ({values_cols})
                        ON CONFLICT DO NOTHING;
                        """
        self.execute_sql_in_parallel()

    def run_sql(self, parameters: Dict[str, Any]) -> None:
        """
        Run the SQL query with the provided parameters.

        Args:
            parameters (Dict[str, Any]): Parameters to be used in the SQL query.

        Raises:
            AirflowException: If there is an SQL error.
        """
        try:
            parameter = {**parameters['metadata'], **parameters['info']}
            parameter = {k: parameter[k] for k in self.cols_list[2:] if k in parameter}
            parameter['matchId'] = parameters['metadata']['matchId']
            parameter['participants'] = ','.join(parameters['metadata']['participants'])
            self.hook.run(
                sql=self.sql,
                autocommit=self.autocommit,
                parameters=parameter,
                handler=self.handler if self.do_xcom_push else None,
                return_last=self.return_last,
            )
        except Exception as exc:
            raise AirflowException(f'SQL ERROR : {exc}') from exc


class InsertSQLMatchDetailOperator(InsertSQLExecuteQueryOperator):  # pylint: disable=too-many-ancestors
    """
    The detailed information of the match information is stored in the database (game result).
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.hook = self.get_db_hook()

    def execute(self, context: Dict[str, Any]) -> None:
        """
        Execute the SQL query to insert match detail information into the database.

        Args:
            context (Dict[str, Any]): Contains information indicating
            the current DAG execution status in Airflow.
        """
        self.execute_sql_in_parallel()

    def run_sql(self, parameters: Dict[str, Any]) -> None:
        """
        Functions to be executed in parallel in execute_sql_in_parallel().
        Execute query by receiving arguments from INSERT_SQLExecuteQueryOperator object.

        Args:
            parameter (Dict[str, Any]): Parameters to pass to the SQL query.

        Raises:
            AirflowException: If there is an SQL error.
        """
        try:
            if parameters['info']['queueId'] == 420:
                for user_number in range(10):
                    challenges = parameters['info']['participants'][user_number].pop('challenges')
                    parameters['info']['participants'][user_number]['perks'] = \
                        str(parameters['info']['participants'][user_number]['perks'])
                    match_dict = {**parameters['info']['participants'][user_number], **challenges}

                    match_cols = list(match_dict.keys())
                    match_cols = re.sub(r"[\[\]]+", "", str(match_cols).replace("'", '"'))

                    if user_number < 5:
                        tmp_parameter = parameters['info']['teams'][0]
                        bans_champion_id = tmp_parameter['bans'][user_number]['championId']
                        pick_turn = tmp_parameter['bans'][user_number]['pickTurn']
                        obj = tmp_parameter['objectives']
                    else:
                        tmp_parameter = parameters['info']['teams'][1]
                        user_number = user_number % 5
                        bans_champion_id = tmp_parameter['bans'][user_number]['championId']
                        pick_turn = tmp_parameter['bans'][user_number]['pickTurn']
                        obj = tmp_parameter['objectives']

                    data = [parameters['metadata']['matchId']] + \
                           list(match_dict.values()) + \
                           [bans_champion_id] + [pick_turn] + \
                           [obj['baron']['first']] + \
                           [obj['dragon']['first']] + \
                           [obj['inhibitor']['first']] + \
                           [obj['riftHerald']['first']]

                    cols = f'''"matchId",
                              {match_cols},
                              "bansChampionId",
                              "pickTurn",
                              "firstBaron",
                              "firstDragon",
                              "firstInhibitor",
                              "riftHerald"
                              '''
                    value_id = '%s,' * (len(data) - 1) + '%s'
                    sql = f"""INSERT INTO match_by_summoners ({cols})
                                VALUES ({value_id}) ON CONFLICT DO NOTHING;
                                """
                    self.hook.run(
                        sql=sql,
                        autocommit=self.autocommit,
                        parameters=data,
                        handler=self.handler if self.do_xcom_push else None,
                        return_last=self.return_last,
                    )
        except Exception as exc:
            raise AirflowException(f'SQL ERROR : {exc}') from exc


# HACK : If the original data volume is large, save it as a file
def create_cache_champion_table() -> None:
    """
    Regardless of the summoner, champion information is stored in Redis. (save redis db:1)
    """
    sql = f"""SELECT "championId",
                    "championName",
                    "teamPosition",
                    "individualPosition",
                    "playedChampSelectPosition",
                    "kda",
                    "win",
                    "lane",
                    "bansChampionId"
                FROM match_by_summoners
                WHERE puuid
                IN (SELECT distinct ON ("puuid") "puuid"
                    FROM (SELECT "puuid", "name", DATE_TRUNC('hour',"regDate") as "regDate"
                          FROM summoners_puuid
                          ORDER BY "regDate" DESC LIMIT {USERS_NUMBER}) a)"""
    key = "champion_table"
    set_cache(sql, key)


def create_cache_champion_ban_table() -> None:
    """
    Store champion ban information in Redis. (save redis db:1)
    """
    data_frame = pd.DataFrame(get_cache("champion_table"),
                              columns=["championId", "championName",
                                       "teamPosition", "individualPosition",
                                       "playedChampSelectPosition", "kda",
                                       "win", "lane", "bansChampionId"])[
        ['bansChampionId', 'championId']]
    df_half_size = len(data_frame) / 2
    ban = data_frame.groupby('bansChampionId').count().reset_index().rename(
        columns={'championId': 'ban_rate'}
    )
    ban['ban_rate'] = ban['ban_rate'] / df_half_size
    key = "champion_ban_table"
    set_cache(None, key, ban)


def op_champ_calculator(data_frame: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate an arbitrary weighted average of minmax scaled data for champion ranks.

    Args:
        data_frame: A pandas DataFrame containing champion data.

    Returns:
        A pandas DataFrame with calculated champion rank.
    """

    data_frame['result'] = (data_frame['scaled_win'] * 4) + \
                           (data_frame['scaled_ban'] * 1) + \
                           (data_frame['scaled_pick'] * 4) + \
                           data_frame['scaled_kda']
    return data_frame


def create_cache_best_champion_table(line: int) -> None:
    """
    Select the best champion and save the final result in Redis.

    Args:
        line: An integer representing one of the following roles:
        {1: 'TOP', 2: 'JUNGLE', 3: 'MIDDLE', 4: 'BOTTOM', 5: 'UTILITY'}.
    """

    line_dict = {1: 'TOP', 2: 'JUNGLE', 3: 'MIDDLE', 4: 'BOTTOM', 5: 'UTILITY'}
    line = line_dict[line]

    data_frame = pd.DataFrame(get_cache("champion_table"),
                              columns=["championId", "championName",
                                       "teamPosition", "individualPosition",
                                       "playedChampSelectPosition", "kda",
                                       "win", "lane",
                                       "bansChampionId"])

    line_df = data_frame.loc[data_frame['teamPosition'] == line]
    count_df = line_df.groupby('championName').count().reset_index()
    df_szie = len(line_df)
    count_df['pick_rate'] = count_df['championId'] / df_szie
    count_df = count_df[['championName', 'championId', 'pick_rate']]
    count_df = count_df.rename(columns={'championId': 'count'})
    line_df = line_df.merge(count_df, on='championName')
    line_df = line_df[['championId', 'championName', 'kda', 'win', 'pick_rate', 'count']].groupby(
        'championId').mean().reset_index()

    ban_df = pd.DataFrame(get_cache("champion_ban_table"))
    line_df = line_df.merge(ban_df,
                            left_on='championId',
                            right_on='bansChampionId',
                            how='left').fillna(0)
    line_df = line_df.loc[line_df['count'] >= 30].sort_values('win')
    line_df['scaled_kda'] = minmax_scaler(line_df['kda'])
    line_df['scaled_win'] = minmax_scaler(line_df['win'])
    line_df['scaled_ban'] = minmax_scaler(line_df['ban_rate'])
    line_df['scaled_pick'] = minmax_scaler(line_df['pick_rate'])
    line_df = op_champ_calculator(line_df)
    line_df['win'] = round(line_df['win'] * 100, 1)
    line_df['ban_rate'] = round(line_df['ban_rate'] * 100, 1)
    line_df['pick_rate'] = round(line_df['pick_rate'] * 100, 1)
    line_df['kda'] = round(line_df['kda'], 1)
    line_df = line_df.sort_values('result', ascending=False).reset_index(drop=True).head(5)
    set_cache(None, f'best_{line}', line_df)


def match_all_table_query(cursor: Any, path: str) -> None:
    """
    Retrieve match information from the database and save it as a file.

    Args:
        cursor: A database cursor.
        path: A string representing the local server data storage location
        (all match information).
    """

    cursor.execute("""
    SELECT * 
    FROM match_by_summoners 
    WHERE puuid 
    IN (SELECT distinct ON ("puuid") "puuid"
        FROM (SELECT "puuid", "name", DATE_TRUNC('hour',"regDate") as "regDate" 
                FROM summoners_puuid
                ORDER BY "regDate" DESC LIMIT 300) a)""")
    data = cursor.fetchall()

    # TODO When querying Columne from database
    # cursor.execute(f"SELECT column_name
    # FROM information_schema.columns
    # WHERE table_name = 'match_by_summoners'")
    # columns = [row[0] for row in cursor.fetchall()]

    columns = [description[0] for description in cursor.description]
    data = pd.DataFrame(data, columns=columns)
    data.to_parquet(f"{path}/all_match_data.parquet")


def match_sp_table_query(target: str, cursor: Any, path: str) -> None:
    """
    Find the largest number of special event users and save the data locally.

    Args:
        target: A string representing either 'puuid' or 'championId'.
        cursor: A database cursor.
        path: A string representing the local server data storage location (Most user or champion).
    """

    sql = f"""
        select "{target}", count(*),
           SUM("inhibitorKills") as "inhibitorKills",
           SUM("turretKills") as "turretKills",
           SUM("damagePerMinute") as "damagePerMinute",
           SUM("visionScorePerMinute") as "visionScorePerMinute",
           SUM(CASE WHEN "firstBloodKill" = TRUE THEN 1 ELSE 0 END) as "firstBloodKill",
           SUM(CASE WHEN "firstTowerKill" = TRUE THEN 1 ELSE 0 END) as "firstTowerKill",
           SUM("pentaKills") as "pentaKills",
           SUM("soloKills") as "soloKills",
           SUM("quickSoloKills") as "quickSoloKills",
           SUM("soloBaronKills") as "soloBaronKills"
    from match_by_summoners where puuid in (SELECT distinct ON ("puuid") "puuid"
              FROM (SELECT "puuid", "name", DATE_TRUNC('hour',"regDate") as "regDate" FROM summoners_puuid
              ORDER BY "regDate" DESC LIMIT {USERS_NUMBER}) a)  
    group by "{target}" 
    """
    cursor.execute(sql)
    columns = [f"{target}",
               'count',
               'inhibitorKills',
               'turretKills',
               'damagePerMinute',
               'visionScorePerMinute',
               'firstBloodKill',
               'firstTowerKill',
               'pentaKills',
               'soloKills',
               'quickSoloKills',
               'soloBaronKills']
    data = cursor.fetchall()
    data = pd.DataFrame(data, columns=columns)
    data.to_parquet(f"{path}/sp_table_by_{target}.parquet")


def top_player_table_query(cursor: Any, path: str) -> None:
    """
    Store data compared to the top user per line locally.

    Args:
        cursor: A database cursor.
        path: A string representing the local server data storage location (1st user).
    """

    sql = f"""
    SELECT "puuid", "teamPosition",
        "gameLength", "win", 
        "kda", "totalDamageDealtToChampions", 
        "totalDamageTaken", "goldPerMinute" 
        FROM match_by_summoners
        WHERE puuid 
        IN (SELECT distinct ON ("puuid") "puuid"
              FROM (SELECT "puuid", "name", DATE_TRUNC('hour',"regDate") as "regDate" 
                    FROM summoners_puuid
                    ORDER BY "regDate" DESC LIMIT {USERS_NUMBER}) a);"""
    cursor.execute(sql)
    data = cursor.fetchall()
    cols = ["puuid", "Position",
            "Game Length", "Win",
            "KDA", "Damage",
            "Damage Taken", "Gold"]
    data = pd.DataFrame(data, columns=cols)
    data.to_parquet(f"{path}/top_player_table.parquet")


def match_by_puuid(path: str) -> None:
    """
    Read all match information data files and divide them into match data files for each user.

    Args:
        path: A string representing the local server data storage location
        (Match information by user).
    """

    data_frame = pd.read_parquet(f"{path}/all_match_data.parquet")
    redis_server = redis_conn(redis_db_number=2)
    puuids = [i.decode('utf-8') for i in redis_server.keys()]
    for puuid in puuids:
        data_frame.loc[data_frame['puuid'] == puuid].to_parquet(
            f"{path}/summoners_match/{puuid}.parquet"
        )


def match_data_to_parquet_task() -> None:
    """
    Store data locally and in Redis.
    """

    try:
        conn = postgres_conn()
        cursor = conn.cursor()
        path = Variable.get('data_path')
        top_player_table_query(cursor, path)
        match_all_table_query(cursor, path)
        match_sp_table_query('puuid', cursor, path)
        match_sp_table_query('championId', cursor, path)
        match_by_puuid(path)
    except Exception as exc:
        raise AirflowException(f'SQL ERROR : {exc}') from exc
    finally:
        conn.close()


@provide_session
def update_last_updated(session: Any = None) -> None:
    """
    Update the last_updated variable in Airflow.

    Args:
        session: An optional session object.
    """

    last_updated = Variable.get("last_updated", default_var=None, deserialize_json=True)
    if not last_updated:
        last_updated = ""
    last_updated = pendulum.now().strftime("%Y-%m-%d %H:%M:%S")
    Variable.set("last_updated", value=last_updated, serialize_json=True, session=session)


# This DAG receives user information of 300 challengers from the Riot API.
#
# Notes on usage:
#
# Turn on all dags.
#
# This DAG runs every 1 hours.
# It doesn't matter if this DAG doesn't run on a schedule.
#
# select_summoners_puuid_task : Get summoner puuid from Database. (300 challengers.)
#
# fetch_summoners_match_id_task : Get summoner matchId from the Riot API.
#
# fetch_match_information_task : Get summoner match information from the Riot API.
#
# insert_match_results_task : Save the summoner match information to Database.
# (Match result)
#
# insert_match_deteil_results_task : Save the summoner match information to Database.
# (Match detail result)
#
# match_data_to_parquet_task : Save the summoner match information to parquet.
#
# champion_table_caching : Save the summoner champion ranking information to redis.
#
# champion_ban_table_caching : Save the summoner champion ban information to redis.
#
# update_timestamp : Update Complete timestamp

with DAG(
        dag_id="challenger_leagues_match",
        schedule="0 */1 * * *",
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        catchup=False,
        tags=["RIOT", "api", "data_collection"]
) as dag:
    select_summoners_puuid_task = SQLExecuteQueryOperator(
        task_id='select_summoners_puuid_task',
        conn_id='postgres_conn',
        sql='SELECT "puuid" FROM summoners_puuid ORDER BY "regDate" DESC LIMIT 300',
    )

    fetch_summoners_match_id_task = FetchSummonerMatchIdOperator(
        task_id='fetch_summoners_match_id_task',
        http_conn_id='http_id_challengerleagues_match',
        method='GET',
        data=select_summoners_puuid_task.output,
        headers={"api_key": Variable.get("riot_api_key")},
        response_check=response_check,
    )

    fetch_match_information_task = FetchMatchInformationOperator(
        task_id='fetch_match_information_task',
        http_conn_id='http_id_challengerleagues_match_information',
        method='GET',
        data=fetch_summoners_match_id_task.output,
        headers={"api_key": Variable.get("riot_api_key")},
        response_check=response_check,
    )

    insert_match_results_task = InsertSQLMatchResultOperator(
        task_id='insert_match_results_task',
        conn_id='postgres_conn',
        sql=None,
        parameters=fetch_match_information_task.output
    )

    insert_match_deteil_results_task = InsertSQLMatchDetailOperator(
        task_id='insert_match_deteil_results_task',
        conn_id='postgres_conn',
        sql=None,
        parameters=fetch_match_information_task.output
    )

    match_data_to_parquet_task = PythonOperator(
        task_id='match_data_to_parquet_task',
        python_callable=match_data_to_parquet_task
    )

    champion_table_caching = PythonOperator(
        task_id='champion_table_caching',
        python_callable=create_cache_champion_table,
        # TODO : column unification
        # op_kwargs={'cols': ''}
    )

    champion_ban_table_caching = PythonOperator(
        task_id='champion_ban_table_caching',
        python_callable=create_cache_champion_ban_table,
    )

    with TaskGroup("champion_table_caching_group") as table_caching_group:
        for i in range(1, 6):
            PythonOperator(task_id=f'table_caching_{i}',
                           python_callable=create_cache_best_champion_table, dag=dag,
                           op_kwargs={'line': i}
                           )

    update_timestamp = PythonOperator(
        task_id='update_timestamp',
        python_callable=update_last_updated,
    )

    select_summoners_puuid_task >> fetch_summoners_match_id_task >> fetch_match_information_task >> [insert_match_results_task, insert_match_deteil_results_task] >> match_data_to_parquet_task  # pylint: disable=line-too-long pointless-statement
    match_data_to_parquet_task >> [champion_table_caching, champion_ban_table_caching] >> table_caching_group >> update_timestamp  # pylint: disable=line-too-long pointless-statement
