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

""" Common module functions """
from concurrent.futures import ThreadPoolExecutor
from typing import Union, List, Dict, Any
import time
import json
import redis
import requests
import psycopg2
import numpy as np
import pandas as pd

from airflow import AirflowException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.operator_helpers import determine_kwargs
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable




PARALLELISM = 2
USE_PARALLELISM = True
HTTP_API_RETRY_WAIT_TIME = 10
DEFAULT_REDIS_DATABASE = 1
USERS_NUMBER = 300


class RateLimitedHttpHook(HttpHook):
    """This class inherits the Airflow HttpHook and makes a request to the Riot API.

    It overrides the check_response method to handle rate limiting,
    and will raise an AirflowException
    if a non-200 or non-429 response is received.

    Args:
        HttpHook (class): The Airflow HttpHook class that this class is inheriting.
    """

    def check_response(self, response: requests.Response) -> None:
        """Checks the status code and raise an AirflowException exception on non 429 or 200
        status codes.

        If the status code is 429, it means that the rate limit has been exceeded and the request
        should be retried after waiting for the appropriate amount of time. If the status code is
        not 429 or 200, an AirflowException will be raised.

        Args:
            response (requests.Response): A requests response object
        Raises:
            AirflowException: If the response status code is not 200 or 429.
        """
        try:
            if response.status_code in (429, 200):
                pass
            else:
                response.raise_for_status()

        except Exception as exc:
            self.log.error("HTTP error: %s", response.reason)
            self.log.error(response.text)
            raise AirflowException(str(response.status_code) + ':' + response.reason) from exc


class BaseFetchOperator(SimpleHttpOperator):
    """
    This class inherits the Airflow SimpleHttpOperator and provides a base implementation
    for making HTTP API requests and processing the response. It can be used as a base class for
    operators that need to fetch data from APIs.

    Args:
        SimpleHttpOperator (SimpleHttpOperator): The Airflow SimpleHttpOperator
        class that this class is inheriting.
    """

    def __init__(self, *args, retry_wait_time: int = HTTP_API_RETRY_WAIT_TIME,
                 use_parallelism: bool = USE_PARALLELISM,
                 **kwargs) -> None:
        """
        Constructs a new BaseFetchOperator.

        Args:
            retry_wait_time (int): The time to wait (in seconds) before
            retrying the API request in case of HTTP 429 response.
            use_parallelism (bool): Whether to use parallel execution
            to process multiple API endpoints concurrently.
            args: Additional positional arguments to pass to the parent constructor.
            kwargs: Additional keyword arguments to pass to the parent constructor.
        """
        super().__init__(*args, **kwargs)
        self.parallelism = PARALLELISM
        self.retry_wait_time = retry_wait_time
        self.use_parallelism = use_parallelism
        self.http_hook = RateLimitedHttpHook(
            self.method,
            http_conn_id=self.http_conn_id,
            auth_type=self.auth_type,
            tcp_keep_alive=self.tcp_keep_alive,
            tcp_keep_alive_idle=self.tcp_keep_alive_idle,
            tcp_keep_alive_count=self.tcp_keep_alive_count,
            tcp_keep_alive_interval=self.tcp_keep_alive_interval,
        )

    def http_hook_response(self, context: Dict[str, Any], endpoint: str = None):
        """
        Sends an HTTP API request using the underlying HttpHook and processes the response.

        Args:
            context (Dict[str, Any]): The Airflow task context dictionary.
            endpoint (str): The API endpoint to request.
            Only used if use_parallelism is False.

        Returns:
            The API response data, filtered according
            to the response_filter function if one is provided.
        """
        while 1:
            if self.use_parallelism:
                response = self.http_hook.run(
                    f'/{endpoint}',
                    self.headers,
                    self.extra_options
                )
            else:
                response = self.http_hook.run(self.endpoint,
                                              self.data,
                                              self.headers,
                                              self.extra_options)

            state = self.response_check(response)
            if state == 200:
                if self.response_filter:
                    kwargs = determine_kwargs(self.response_filter, [response], context)
                    return self.response_filter(response, **kwargs)
                return response.json()
            if state == 429:
                self.log.info(f"HTTP 429 Too Many Requests {self.retry_wait_time} sec waits....")
                time.sleep(self.retry_wait_time)
            else :
                raise AirflowException("Response check returned False.")


    def execute(self, context: Dict[str, Any]) -> Dict:
        """
        Executes the operator by sending the
        HTTP API request(s) and processing the response(s).

        Args:
            context (Dict[str, Any]): The Airflow task context dictionary. (default)

        Returns:
            The API response data(s), filtered according
            to the response_filter function if one is provided.


        HTTP429_HttpHook parameter.
        :param method: the API method to be called
        :param http_conn_id: :ref:`http connection<howto/connection:http>` that has the base
            API url i.e https://www.google.com/ and optional authentication credentials.
            Default headers can also be specified in the Extra field in json format.
        :param auth_type: The auth type for the service
        :param tcp_keep_alive: Enable TCP Keep Alive for the connection.
        :param tcp_keep_alive_idle: The TCP Keep Alive Idle parameter
        (corresponds to ``socket.TCP_KEEPIDLE``).
        :param tcp_keep_alive_count: The TCP Keep Alive count parameter
        (corresponds to ``socket.TCP_KEEPCNT``)
        :param tcp_keep_alive_interval: The TCP Keep Alive interval parameter
        (corresponds to``socket.TCP_KEEPINTVL``)
        """
        if self.use_parallelism:
            results = self.execute_request_in_parallel(context)
            return results

        results = self.http_hook_response(context)
        return results

    def execute_request_in_parallel(self, context: Dict[str, Any]):
        """
        Executes the operator by sending the HTTP API request(s)
        and processing the response(s) in parallel.

        Args:
            context (Dict[str, Any]): The Airflow task context dictionary.

        Returns:
            The API response data(s), filtered according
            to the response_filter function if one is provided.
        """
        with ThreadPoolExecutor(max_workers=self.parallelism) as executor:
            futures = [executor.submit(self.run_request,
                                       endpoint[0],
                                       context) for endpoint in self.data]  # count data
            results = [future.result() for future in futures]
        return results

    def run_request(self, endpoint: str, context: Dict[str, Any]):
        """
        Executes an HTTP API request and returns the response data.

        Args:
            endpoint (str): The API endpoint to request.
            context (Dict[str, Any]): The Airflow task context dictionary.

        Returns:
            The API response data, filtered according
            to the response_filter function if one is provided.
        """
        results = self.http_hook_response(context, endpoint)
        return results


class InsertSQLExecuteQueryOperator(SQLExecuteQueryOperator):
    """
    This class inherits Airflow SQLExecuteQueryOperator and executes an insert query.

    Args:
        query_type (Callable[[str, str], str]): A function to generate SQL query.
    """

    def __init__(self, query_type: str = None, **kwargs):
        super().__init__(**kwargs)
        self.query_type = query_type
        self.parallelism = PARALLELISM
        self.hook = self.get_db_hook()

    def parameters_query(self, parameters: Union[List[Dict[str, Any]],
                                                 Dict[str, Any]],
                         is_col: bool = False) -> (str, str):
        """
        Preprocessing function to turn previous task return value into query statement.

        Args:
            parameters (Union[List[Dict[str, Any]], Dict[str, Any]]): List or dictionary.
            is_col (bool): Indicates if the columns are already processed.

        Returns:
            (str, str): Separation of columns to be inserted and data
            to be inserted in a list or dictionary.
        """
        if not is_col:
            if isinstance(parameters, list):
                parameters_cols = parameters[0].keys()
            else:
                parameters_cols = parameters.keys()
        else:
            parameters_cols = parameters

        select_cols = ", ".join([f'"{col}"' for col in parameters_cols])
        values_cols = ", ".join([f"%({col})s" for col in parameters_cols])
        return select_cols, values_cols

    def execute(self, context: Dict[str, Any]) -> None:
        """
        A function that executes a query statement.

        Args:
            context (Dict[str, Any]): Contains information indicating
            the current DAG execution status in Airflow.

        ```python
        def execute(self, context):
                self.log.info("Executing: %s", self.sql)
                hook = self.get_db_hook()
                if self.split_statements is not None:
                    extra_kwargs = {"split_statements": self.split_statements}
                else:
                    extra_kwargs = {}
                output = hook.run(
                    sql=self.sql,
                    autocommit=self.autocommit,
                    parameters=self.parameters,
                    handler=self.handler if self.do_xcom_push else None,
                    return_last=self.return_last,
                    **extra_kwargs,
                )
                if not self.do_xcom_push:
                    return None
                if return_single_query_results(self.sql, self.return_last, self.split_statements):
                    # For simplicity, we pass always list as input to _process_output, regardless if
                    # single query results are going to be returned, and we return the first element
                    # of the list in this case from the (always) list returned by _process_output
                    return self._process_output([output], hook.descriptions)[-1]
                return self._process_output(output, hook.descriptions)

        """

        select_cols, values_cols = self.parameters_query(self.parameters)
        self.sql = self.query_type(select_cols, values_cols)

        self.execute_sql_in_parallel()

    def execute_sql_in_parallel(self) -> None:
        '''
        Parallel processing function
        '''
        with ThreadPoolExecutor(max_workers=self.parallelism) as executor:
            futures = [executor.submit(self.run_sql, parameter) for parameter in self.parameters]
            [future.result() for future in futures] # pylint: disable=expression-not-assigned

    def run_sql(self, parameters: str) -> None:
        """
        Functions to be executed in parallel in execute_sql_in_parallel().
        Execute query by receiving arguments from INSERT_SQLExecuteQueryOperator object.

        Args:
            parameter (Dict[str, Any]): Parameters to pass to the SQL query.
        """
        self.hook.run(
            sql=self.sql,
            autocommit=self.autocommit,
            parameters=parameters,
            handler=self.handler if self.do_xcom_push else None,
            return_last=self.return_last,
        )


def response_check(response: requests.Response) -> Union[int, bool]:
    """
    A function that checks if the return value is 200,429 after requesting the api.

    Args:
        response (Response): A requests response object.

    Returns:
        Union[int, bool]: status result.
    """
    if response.status_code == 200:
        return 200
    if response.status_code == 429:
        return 429

    return False


def response_filter(response: requests.Response) -> List[Dict[str, Any]]:
    """
    A function that filters only entry information when querying challenger users
    (fetch summonerId).

    Args:
        response (Response): api request return object.

    Returns:
        List[Dict[str, Any]]: The entries object contains the information of 300 challengers.
    """
    data_list = response.json()['entries']
    return data_list


def set_cache(sql: str,
              key: str,
              data : Union[Dict, pd.DataFrame] = None,
              redis_db_number: int = DEFAULT_REDIS_DATABASE
              ) -> None:
    """
    Args:
        sql (str): SQL query to execute.
        key (str): Key to store the data.
        data (Union[Dict, pd.DataFrame]) : DataFrame.
        redis_db_number (int): Redis database number.
    """
    if sql:
        conn_postgres = postgres_conn()
        cursor = conn_postgres.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        conn_postgres.close()

    conn_redis = redis_conn(redis_db_number)
    if isinstance(data, pd.DataFrame):
        data = data.to_dict()
    conn_redis.set(key, json.dumps(data))
    conn_redis.close()


def get_cache(key: str, redis_db_number: int = DEFAULT_REDIS_DATABASE) -> Dict[str, Any]:
    """
    Args:
        key (str): Key to get the data.
        db (int): Redis database number.

    Returns:
        Dict[str, Any]: Data retrieved from Redis.
    """
    conn_redis = redis_conn(redis_db_number)
    data = conn_redis.get(key)
    conn_redis.close()
    return json.loads(data)


def postgres_conn() -> psycopg2.extensions.connection:
    """
    Returns:
        psycopg2.extensions.connection: Postgres connection object.
    """
    return psycopg2.connect(
        host=Variable.get('postgres_host'),
        database=Variable.get('postgres_db'),
        user=Variable.get('postgres_user'),
        password=Variable.get('postgres_password')
    )


def redis_conn(redis_db_number: int = DEFAULT_REDIS_DATABASE) -> redis.client.Redis:
    """
    Establishes a connection to a Redis instance.

    Args:
        redis_db_number (int, optional): Redis database number. Default is 1.

    Returns:
        redis.Redis: Redis connection object.
    """
    return redis.Redis(host=Variable.get('redis_host'),
                       port=Variable.get('redis_port'),
                       db=redis_db_number)


# from numba import jit
# TODO: airflow celery fails to compile. there is some bug
# @jit(nopython=True)
def minmax_scaler(data: np.ndarray) -> np.ndarray:
    """
    Normalizes the given data using the Min-Max scaling method.

    Args:
        data (np.ndarray): Input data to be scaled.

    Returns:
        np.ndarray: Scaled data with values between 0 and 1.
    """
    min_val = np.min(data)
    max_val = np.max(data)
    return (data - min_val) / (max_val - min_val)
