#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example DAG demonstrating the usage of the PigOperator."""

from airflow import DAG
from airflow.providers.apache.kylin.operators.kylin import KylinOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id='example_kylin_operator',
    default_args=args,
    schedule_interval=None,
    tags=['example']
)


def gen_build_time(**kwargs):
    ti = kwargs['ti']
    ti.xcom_push(key='date_start', value='1325347200000')
    ti.xcom_push(key='date_end', value='1325433600000')


gen_build_time_task = PythonOperator(
    python_callable=gen_build_time,
    task_id='gen_build_time',
    dag=dag
)

build_task1 = KylinOperator(
    task_id="kylin_build_1",
    kylin_conn_id='kylin_default',
    project='learn_kylin',
    cube='kylin_sales_cube',
    command='build',
    start_time="{{ task_instance.xcom_pull(task_ids='gen_build_time',key='date_start') }}",
    end_time="{{ task_instance.xcom_pull(task_ids='gen_build_time',key='date_end') }}",
    is_track_job=True,
    dag=dag,
)

build_task2 = KylinOperator(
    task_id="kylin_build_2",
    kylin_conn_id='kylin_default',
    project='learn_kylin',
    cube='kylin_sales_cube',
    command='build',
    start_time='1325433600000',
    end_time='1325520000000',
    is_track_job=True,
    dag=dag,
)

refresh_task1 = KylinOperator(
    task_id="kylin_refresh_1",
    kylin_conn_id='kylin_default',
    project='learn_kylin',
    cube='kylin_sales_cube',
    command='refresh',
    start_time='1325347200000',
    end_time='1325433600000',
    is_track_job=True,
    dag=dag,
)

merge_task = KylinOperator(
    task_id="kylin_merge",
    kylin_conn_id='kylin_default',
    project='learn_kylin',
    cube='kylin_sales_cube',
    command='merge',
    start_time='1325347200000',
    end_time='1325520000000',
    is_track_job=True,
    dag=dag,
)

disable_task = KylinOperator(
    task_id="kylin_disable",
    kylin_conn_id='kylin_default',
    project='learn_kylin',
    cube='kylin_sales_cube',
    command='disable',
    dag=dag,
)

purge_task = KylinOperator(
    task_id="kylin_purge",
    kylin_conn_id='kylin_default',
    project='learn_kylin',
    cube='kylin_sales_cube',
    command='purge',
    dag=dag,
)

build_task3 = KylinOperator(
    task_id="kylin_build_3",
    kylin_conn_id='kylin_default',
    project='learn_kylin',
    cube='kylin_sales_cube',
    command='build',
    start_time='1325433600000',
    end_time='1325520000000',
    dag=dag,
)

gen_build_time_task >> build_task1 >> build_task2 >> refresh_task1 >> merge_task
merge_task >> disable_task >> purge_task >> build_task3

streaming_build_task1 = KylinOperator(
    task_id="kylin_build_streaming_1",
    kylin_conn_id='kylin_default',
    project='learn_kylin',
    cube='kylin_streaming_cube',
    command='build_streaming',
    offset_start='0',
    offset_end='9223372036854775807',
    is_track_job=True,
    dag=dag,
)

streaming_build_task2 = KylinOperator(
    task_id="kylin_build_streaming_2",
    kylin_conn_id='kylin_default',
    project='learn_kylin',
    cube='kylin_streaming_cube',
    command='build_streaming',
    offset_start='0',
    offset_end='9223372036854775807',
    is_track_job=True,
    dag=dag,
)

streaming_merge_task = KylinOperator(
    task_id="kylin_merge_streaming",
    kylin_conn_id='kylin_default',
    project='learn_kylin',
    cube='kylin_streaming_cube',
    command='merge_streaming',
    offset_start='0',
    offset_end='9223372036854775807',
    is_track_job=True,
    dag=dag,
)

streaming_build_task1 >> streaming_build_task2 >> streaming_merge_task
