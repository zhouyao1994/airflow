# -*- coding: utf-8 -*-
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

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.kylin.hooks.kylin_hook import KylinHook


class KylinOperator(BaseOperator):
    ui_color = '#B03060'
    template_fields = (
        'is_kylin', 'rest_api_version', 'ke_version', 'conn_id', 'project', 'cube', 'op_mod', 'build_type',
        'track_job_status', 'start_time', 'end_time', 'source_offset_start', 'source_offset_end',
        'force_merge_empty_segment', 'segment_name', 'job_id', 'sleep_time', 'time_out', 'sql',
        'query_result_xcom_push', 'mpvalues', 'force', 'segments', 'lookup_table', 'segment_id', 'hdfs_path',
        'mkdir_on_hdfs', 'table_mapping', 'point_list', 'range_list',
        'entity', 'cache_key', 'event', 'model', 'ids', 'purge', 'affected_start', 'affected_end',
        'model_mode', 'endpoint', 'method', 'data', 'headers')
    template_ext = ()

    @apply_defaults
    def __init__(self,
                 is_kylin=True,
                 rest_api_version='v2',
                 ke_version='3.0',
                 conn_id='kylin_default',
                 project=None,
                 cube=None,
                 op_mod=None,
                 build_type=None,
                 track_job_status=False,
                 start_time=None,
                 end_time=None,
                 source_offset_start=0,
                 source_offset_end=None,
                 force_merge_empty_segment=False,
                 segment_name=None,
                 job_id=None,
                 sleep_time=60,
                 time_out=1600000000,
                 sql=None,
                 query_result_xcom_push=False,
                 mpvalues=None,
                 force=False,
                 segments=None,
                 lookup_table=None,
                 segment_id=None,
                 hdfs_path=None,
                 mkdir_on_hdfs=None,
                 table_mapping=None,
                 point_list=None,
                 range_list=None,
                 entity=None,
                 cache_key=None,
                 event=None,
                 model=None,
                 ids=None,
                 purge=None,
                 affected_start=None,
                 affected_end=None,
                 model_mode=None,
                 endpoint=None,
                 method=None,
                 data=None,
                 headers=None,
                 *args,
                 **kwargs):
        super(KylinOperator, self).__init__(*args, **kwargs)
        self.is_kylin = is_kylin
        self.rest_api_version = rest_api_version
        self.ke_version = ke_version
        self.conn_id = conn_id
        self.project = project
        self.cube = cube
        self.op_mod = op_mod
        self.build_type = build_type
        self.track_job_status = track_job_status
        self.start_time = start_time
        self.end_time = end_time
        self.source_offset_start = source_offset_start
        self.source_offset_end = source_offset_end
        self.force_merge_empty_segment = force_merge_empty_segment
        self.segment_name = segment_name
        self.job_id = job_id
        self.sleep_time = sleep_time
        self.time_out = time_out
        self.sql = sql
        self.query_result_xcom_push = query_result_xcom_push
        self.mpvalues = mpvalues
        self.force = force
        self.segments = segments
        self.lookup_table = lookup_table
        self.segment_id = segment_id
        self.hdfs_path = hdfs_path
        self.mkdir_on_hdfs = mkdir_on_hdfs
        self.table_mapping = table_mapping
        self.point_list = point_list
        self.range_list = range_list
        self.entity = entity
        self.cache_key = cache_key
        self.event = event
        self.model = model
        self.ids = ids
        self.purge = purge
        self.affected_start = affected_start
        self.affected_end = affected_end
        self.model_mode = model_mode
        self.endpoint = endpoint
        self.method = method
        self.data = data
        self.headers = headers
        self.other_parameters = kwargs

    def execute(self, context):
        hook = KylinHook(
            is_kylin=self.is_kylin,
            rest_api_version=self.rest_api_version,
            ke_version=self.ke_version,
            conn_id=self.conn_id,
            project=self.project,
            cube=self.cube,
            op_mod=self.op_mod,
            build_type=self.build_type,
            track_job_status=self.track_job_status,
            start_time=self.start_time,
            end_time=self.end_time,
            source_offset_start=self.source_offset_start,
            source_offset_end=self.source_offset_end,
            force_merge_empty_segment=self.force_merge_empty_segment,
            segment_name=self.segment_name,
            job_id=self.job_id,
            sleep_time=self.sleep_time,
            time_out=self.time_out,
            sql=self.sql,
            query_result_xcom_push=self.query_result_xcom_push,
            mpvalues=self.mpvalues,
            force=self.force,
            segments=self.segments,
            lookup_table=self.lookup_table,
            segment_id=self.segment_id,
            hdfs_path=self.hdfs_path,
            mkdir_on_hdfs=self.mkdir_on_hdfs,
            table_mapping=self.table_mapping,
            point_list=self.point_list,
            range_list=self.range_list,
            entity=self.entity,
            cache_key=self.cache_key,
            event=self.event,
            model=self.model,
            ids=self.ids,
            purge=self.purge,
            affected_start=self.affected_start,
            affected_end=self.affected_end,
            model_mode=self.model_mode,
            endpoint=self.endpoint,
            method=self.method,
            data=self.data,
            headers=self.headers,
            **self.other_parameters)
        result = hook.run()
        if self.op_mod == "query" and self.query_result_xcom_push:
            return result

    # # def on_kill(self):
    # #     pass
    # #
    # # def pre_execute(self, context):
    # #     pass
    # #
    # # def post_execute(self, context, result=None):
    # #     pass
