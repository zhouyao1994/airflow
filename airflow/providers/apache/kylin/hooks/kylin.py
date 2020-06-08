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

from typing import Optional
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from kylinpy import kylinpy
from kylinpy import exceptions


class KylinHook(BaseHook):
    """
    :param kylin_conn_id: The connection id as configured in Airflow administration.
    :type kylin_conn_id: str
    :param project: porject name
    :type project: Optional[str]
    :param dsn: dsn
    :type dsn: Optional[str]
    """
    def __init__(self,
                 kylin_conn_id: Optional[str] = 'kylin_default',
                 project: Optional[str] = None,
                 dsn: Optional[str] = None
                 ):
        super().__init__()
        self.kylin_conn_id = kylin_conn_id
        self.project = project
        self.dsn = dsn

    def get_conn(self):
        conn = self.get_connection(self.kylin_conn_id)
        if self.dsn:
            return kylinpy.create_kylin(self.dsn)
        else:
            self.project = self.project if self.project else conn.schema
            return kylinpy.Kylin(conn.host, username=conn.login,
                                 password=conn.password, port=conn.port,
                                 project=self.project, **conn.extra_dejson)

    def cube_run(self, datasource_name, op, **op_args):
        cube_source = self.get_conn().get_datasource(datasource_name)
        try:
            response = cube_source.invoke_command(op, **op_args)
            return response
        except exceptions.KylinError as err:
            raise AirflowException("Cube operation {} error , Message: {}".format(op, err))

    def get_job_status(self, job_id):
        return self.get_conn().get_job(job_id).status
