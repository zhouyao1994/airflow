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

import json
import time
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.providers.http.hooks.http import HttpHook


class KylinHook(BaseHook):
    """
    :param is_kylin: True: apache kylin, Flase: kyligence
    :type is_kylin: bool
    :param version: restful api version (can be v2, v4)
    :type version:str
    :param conn_id: The connection id as configured in Airflow administration.
    :type conn_id: str
    :param project: porject name
    :type project: str
    :param cube: cube name
    :type cube: str
    :param op_mod: operation
        Apache Kylin:
            build: normal build request
            build2: streaming build
            query: query request
        Kyligence:
            build: normal build request
            build_streaming: streaming build
            query: query request
    :type op_mod: str
    :param build_type:
        Apache Kylin:
            if op_mod is build:
                BUILD,FRESH,MERGE
        Kyligence:
            build:
                v2:BUILD,FRESH,MERGE,BUILD_CUSTOMIZED,BATCH_SYNC,REFRESH_LOOKUP
                v4:BUILD,FRESH,MERGE
    :type build_type: str
    :param track_job_status: Whether to track job status after submit a build request
    :type track_job_status: bool
    :param start_time: segment's start-time
    :type start_time: long
    :param end_time: segment's end-time
    :type end_time: long
    :param source_offset_start: build start offset (ke streaming build or custome build)
    :type source_offset_start: long
    :param source_offset_end: build end offset
    :type source_offset_end: long
    :param force_merge_empty_segment: apachekylin merge whether to force merge empty segment
    :type force_merge_empty_segment: bool
    :param segment_name: The segment to refresh or delete
    :type segment_name: str
    :param job_id: job id
    :type job_id: str
    :param sleep_time:
    :type sleep_time: long
    :param time_out: max job running time
    :type time_out: long
    :param sql:
    :type sql: str
    :param query_result_xcom_push: Whether to push query result to xcom
    :type query_result_xcom_push: bool
    :param mpvalues:
    :type mpvalues:
    :param force: ke whether force build
    :type force: bool
    :param segments: segments to be operated
    :type  segments: list
    :param lookup_table: lookup table
    :type lookup_table: str
    :param segment_id: segment id
    :type segment_id: str
    :param hdfs_path: [ke] import or export segments
    :type hdfs_path:str
    :param mkdir_on_hdfs:export segments, force to make dir on hdfs
    :type mkdir_on_hdfs: bool
    :param table_mapping: import segments
    :type table_mapping: list
    :param point_list: batch build
    :type point_list: list
    :param range_list: batch build
    :type range_list: list
    :param entity: clear cache
    :type entity: str
    :param cache_key: clear cache
    :type cache_key: str
    :param event: clear cache
    :type event: str
    :param model: model name
    :type model: str
    :param ids: segment ids
    :type  ids: list
    :param purge: whether purge segment
    :type purge: bool
    :param affected_start: [ke v4 smart mode] refresh segments
    :type affected_start: str
    :param affected_end:
    :type affected_end: str
    :param model_mode: [ke V4] (AI_AUGMENTED_MODE,SMART_MODE)
    :type model_mode: str
    :param endpoint: http endpoint
    :type endpoint: str
    :param method: http method ( PUT,POST,GET...)
    :type method: str
    :param data: http data
    :type data: dict
    :param headers: http header
    :type headers: dict
    """
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
                 **args
                 ):
        self.is_kylin = is_kylin
        self.rest_api_version = rest_api_version.lower()
        self.ke_version = ke_version
        self.conn_id = conn_id
        self.project = project
        self.cube = cube
        self.op_mod = op_mod
        self.build_type = build_type.upper()
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
        self.lookupTable = lookup_table
        self.segment_id = segment_id
        self.hdfs_path = hdfs_path
        self.mkdir_on_hdfs = mkdir_on_hdfs
        self.table_mapping = table_mapping or {}
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
        self.endpoint = endpoint[:-1] if endpoint is not None and endpoint[-1] == '/' else endpoint
        self.method = method
        self.data = data or {}
        self.headers = headers or {}
        self.hook = None

    def _gen_self_http_hook(self):
        if not self.hook:
            self.hook = HttpHook(method=self.method, http_conn_id=self.conn_id)

    def _gen_build(self):
        self.method = "PUT"
        self.endpoint = "/kylin/api/cubes/{cube_name}/build".format(cube_name=self.cube)
        self.data = {"startTime": self.start_time,
                     "endTime": self.end_time,
                     "buildType": self.build_type,
                     "forceMergeEmptySegment": self.force_merge_empty_segment}
        self.headers = {"Content-Type": "application/json"}

    def _gen_build2(self):
        self.method = "PUT"
        self.endpoint = "/kylin/api/cubes/{cube_name}/build2".format(cube_name=self.cube)
        self.data = {"sourceOffsetStart": self.source_offset_start,
                     "sourceOffsetEnd": self.source_offset_end,
                     "buildType": self.build_type}
        self.headers = {"Content-Type": "application/json"}

    def _gen_delete_segment(self):
        self.method = "DELETE"
        self.endpoint = "/kylin/api/cubes/{cube_name}/segs/{segment_name}".format(cube_name=self.cube,
                                                                                  segment_name=self.segment_name)

    def _gen_query(self, sql):
        self.method = "PUT"
        self.endpoint = "/kylin/api/query"
        self.data = {"sql": self.sql, "project": self.project}
        self.headers = {"Content-Type": "application/json"}

    def _gen_apache_kylin_request(self):
        if self.op_mod == "build":
            self._gen_build()
        elif self.op_mod == "build2":
            self._gen_build2()
        elif self.op_mod == "query":
            self._gen_query()

    def _gen_ke_build_v2(self):
        self.method = "PUT"
        self.endpoint = "/kylin/api/cubes/{cube_name}/segments/build".format(cube_name=self.cube)
        self.data = {"startTime": self.start_time,
                     "endTime": self.end_time,
                     "buildType": self.build_type,
                     "force": self.force}
        if self.mpvalues:
            self.data.update({"mpValues": self.mpvalues})

        self.headers = {"Content-Type": "application/json;charset=utf-8",
                        "Accept": "application/vnd.apache.kylin-v2+json",
                        "Accept - Language": "en",
                        }

    def _gen_ke_manage_segments_v2(self):
        self.method = "PUT"
        self.endpoint = "/kylin/api/cubes/{cube_name}/segments".format(cube_name=self.cube)
        self.data = {"segments": self.segments,
                     "buildType": self.build_type,
                     "force": self.force}
        if self.mpvalues:
            self.data.update({"mpValues": self.mpvalues})

        self.headers = {"Content-Type": "application/json;charset=utf-8",
                        "Accept": "application/vnd.apache.kylin-v2+json",
                        "Accept - Language": "en",
                        }

    def _gen_ke_manage_segments_export_v2(self):
        self.method = "POST"
        self.endpoint = "/kylin/api/cubes/segment/export"
        self.data = {"cube": self.cube,
                     "project": self.project,
                     "segmentId": self.segment_id,
                     "hdfsPath": self.hdfs_path}
        if self.mkdir_on_hdfs:
            self.data.update({"mkdirOnHdfs": self.mkdir_on_hdfs})

        self.headers = {"Content-Type": "application/json;charset=utf-8",
                        "Accept": "application/vnd.apache.kylin-v2+json",
                        "Accept - Language": "en",
                        }

    def _gen_ke_manage_segments_import_v2(self):
        self.method = "POST"
        self.endpoint = "/kylin/api/cubes/segment/import"
        self.data = {"hdfsPath": self.hdfs_path}
        if self.cube:
            self.data.update({"cubeName": self.cube})
        if self.project:
            self.data.update({"projectName": self.project})
        if self.table_mapping:
            self.data.update({"tableMapping": self.table_mapping})

        self.headers = {"Content-Type": "application/json;charset=utf-8",
                        "Accept": "application/vnd.apache.kylin-v2+json",
                        "Accept - Language": "en",
                        }


    def _gen_ke_build_streaming_v2(self):
        self.method = "PUT"
        self.endpoint = "/kylin/api/cubes/{cube_name}/segments/build_streaming".format(cube_name=self.cube)
        self.data = {"sourceOffsetStart": self.source_offset_start,
                     "sourceOffsetEnd": self.source_offset_end,
                     "buildType": self.build_type}
        if self.mpvalues:
            self.data.update({"mpValues": self.mpvalues})
        if self.force:
            self.data.update({"force": self.force})

        self.headers = {"Content-Type": "application/json;charset=utf-8",
                        "Accept": "application/vnd.apache.kylin-v2+json",
                        "Accept - Language": "en",
                        }

    def _gen_ke_build_customized_v2(self):
        self.method = "PUT"
        self.endpoint = "/kylin/api/cubes/{cube_name}/segments/build_customized".format(cube_name=self.cube)
        self.data = {"sourceOffsetStart": self.source_offset_start,
                     "sourceOffsetEnd": self.source_offset_end,
                     "buildType": self.build_type}
        if self.mpvalues:
            self.data.update({"mpValues": self.mpvalues})
        if self.force:
            self.data.update({"force": self.force})

        self.headers = {"Content-Type": "application/json;charset=utf-8",
                        "Accept": "application/vnd.apache.kylin-v2+json",
                        "Accept - Language": "en",
                        }

    def _gen_ke_build_batch_sync_v2(self):
        self.method = "PUT"
        self.endpoint = "/kylin/api/cubes/{cube_name}/batch_sync".format(cube_name=self.cube)
        self.data = {"pointList": self.point_list,
                     "rangeList": self.range_list,
                     "buildType": self.build_type}
        if self.mpvalues:
            self.data.update({"mpValues": self.mpvalues})
        if self.force:
            self.data.update({"force": self.force})

        self.headers = {"Content-Type": "application/json;charset=utf-8",
                        "Accept": "application/vnd.apache.kylin-v2+json",
                        "Accept - Language": "en",
                        }

    def _gen_ke_fresh_lookup_v2(self):
        self.method = "PUT"
        self.endpoint = "/kylin/api/cubes/{cube_name}/refresh_lookup".format(cube_name=self.cube)
        self.data = {"lookupTable": self.lookupTable,
                     "project": self.project}
        self.headers = {"Content-Type": "application/json;charset=utf-8",
                        "Accept": "application/vnd.apache.kylin-v2+json",
                        "Accept - Language": "en",
                        }

    def _gen_ke_cache_clean_v2(self):
        self.method = "PUT"
        self.endpoint = "/kylin/api/cache/announce/{entity}/{cache_key}/{event}".format(
            entity=self.entity,
            cache_key=self.cache_key,
            event=self.event
        )
        self.headers = {"Content-Type": "application/json;charset=utf-8",
                        "Accept": "application/vnd.apache.kylin-v2+json",
                        "Accept - Language": "en",
                        }

    def _gen_ke_cache_clean_onenode_v2(self):
        self.method = "PUT"
        self.endpoint = "/kylin/api/cache/{entity}/{cache_key}/{event}".format(
            entity=self.entity,
            cache_key=self.cache_key,
            event=self.event
        )
        self.headers = {"Content-Type": "application/json;charset=utf-8",
                        "Accept": "application/vnd.apache.kylin-v2+json",
                        "Accept - Language": "en",
                        }

    def _gen_ke4_build_v2(self):
        self.method = "PUT"
        self.endpoint = "/kylin/api/cubes/{cube_name}/rebuild".format(cube_name=self.cube)
        self.data = {"startTime": self.start_time,
                     "endTime": self.end_time,
                     "buildType": self.build_type  # "REFRESH","BUILD"
                     }
        self.headers = {"Content-Type": "application/json;charset=utf-8",
                        "Accept": "application/vnd.apache.kylin-v2+json",
                        "Accept - Language": "en",
                        }

    def _gen_ke_build_segment_ai_augmented_mode_v4(self):
        self.method = "POST"
        self.endpoint = "/kylin/api/models/{model_name}/segments".format(
            model_name=self.model,
        )
        self.data = {"project": self.project},

        if self.start_time and self.end_time:
            self.data.update({"start": str(self.start_time),
                             "end": str(self.end_time)})
        elif self.ids:
            self.data.update({"ids": self.ids, "type": self.build_type})
            self.method = "PUT"

        self.headers = {"Content-Type": "application/json;charset=utf-8",
                        "Accept": "application/vnd.apache.kylin-v4-public+json",
                        "Accept - Language": "en",
                        }

    def _gen_ke_purge_segment_ai_augmented_mode_v4(self):
        self.method = "DELETE"
        self.endpoint = "/kylin/api/models/{model_name}/segments?project={project}&ids={ids}&purge={purge}".format(
            model_name=self.model,
            project=self.project,
            ids=self.ids,
            purge=self.purge
        )
        self.headers = {"Content-Type": "application/json;charset=utf-8",
                        "Accept": "application/vnd.apache.kylin-v4-public+json",
                        "Accept - Language": "en",
                        }

    def _gen_ke_build_segment_smart_mode_v4(self):
        self.method = "POST"
        self.endpoint = "/kylin/api/tables/data_range"

        self.data = {"project": self.project,
                     "table": self.table,
                     "start": self.start_time,
                     "end": self.end_time
        }
        self.headers = {"Content-Type": "application/json;charset=utf-8",
                        "Accept": "application/vnd.apache.kylin-v4-public+json",
                        "Accept - Language": "en",
                        }

    # post
    def _gen_ke_fresh_segment_smart_mode_v4(self):
        self.method = "PUT"
        self.endpoint = "/kylin/api/tables/data_range"
        self.data = {"project": self.project,
                     "table": self.table,
                     "refresh_start": self.start_time,
                     "refresh_end": self.end_time,
                     "affected_start": self.affected_start,
                     "affected_end": self.affected_end
                     }

        self.headers = {"Content-Type": "application/json;charset=utf-8",
                        "Accept": "application/vnd.apache.kylin-v4-public+json",
                        "Accept - Language": "en",
                        }

    def _gen_ke_build_indexes_v4(self):
        self.method = "POST"
        self.endpoint = "/kylin/api/models/{model_name}/indexes".format(
            model_name=self.model,
        )
        self.data = {"project": self.project}
        self.headers = {"Content-Type": "application/json;charset=utf-8",
                        "Accept": "application/vnd.apache.kylin-v4-public+json",
                        "Accept - Language": "en",
                        }

    def _gen_kyligence_request_v2(self):
        if self.ke_version[0] == '4':
            if self.op_mod == "build" and (self.build_type == "BUILD" or (
                    self.build_type == "REFRESH" and self.start_time)):
                self._gen_ke4_build_v2()
            elif self.op_mod == "build" and self.segments:
                self._gen_ke_manage_segments_v2()
        elif self.ke_version[0] == '3':
            if self.op_mod == "build":
                if self.build_type == "BUILD":
                    self._gen_ke_build_v2()
                elif self.build_type == "BUILD_CUSTOMIZED":  # "build_customized"
                    self._gen_ke_build_customized_v2()
                elif self.build_type == "BATCH_SYNC":  # "batch_sync"
                    self._gen_ke_build_batch_sync_v2()
                elif self.build_type == "REFRESH_LOOKUP":  # "refresh_lookup"
                    self._gen_ke_fresh_lookup_v2()
            elif self.op_mod == "build_streaming":
                self._gen_ke_build_streaming_v2()

    def _gen_kyligence_request_v4(self):
        if self.model_mode == "SMART_MODE":
            if self.op_mod == "build" and self.build_type == "BUILD" and self.model_mode == "SMART_MODE":
                self._gen_ke_build_segment_smart_mode_v4()
            elif self.op_mod == "build" and self.build_type == "REFRESF" and self.model_mode == "SMART_MODE":
                self._gen_ke_fresh_segment_smart_mode_v4()
        elif self.model_mode == "AI_AUGMENTED_MODE":
            if self.op_mod == "build" and self.build_type == "PURGE":
                self._gen_ke_purge_segment_ai_augmented_mode_v4()
            elif self.op_mod == "build":
                self._gen_ke_build_segment_ai_augmented_mode_v4()
        if self.op_mod == "build" and self.build_type == "INDEXES":
            self._gen_ke_build_indexes_v4()

    def _gen_kyligence_request(self):
        if self.rest_api_version == "v2":
            self._gen_kyligence_request_v2()
        elif self.rest_api_version == "v4":
            self._gen_kyligence_request_v4()

    def _get_job_ids_from_response(self, rsp_json):
        job_ids = []
        rsp_datas = []
        if self.is_kylin:
            rsp_datas = [rsp_json]
        else:
            if isinstance(rsp_json["data"], list) and len(rsp_json["data"]) > 0:
                # ke3 v2 batch_sync
                if "data" in rsp_json["data"][0].keys():
                    rsp_datas = [data["data"] for data in rsp_json["data"]]
                else:
                    # ke3 v2 refresh segments
                    rsp_datas = rsp_json["data"]
            elif isinstance(rsp_json["data"], dict):
                # ke4 v4
                # if "jobs" in rsp_json["data"].keys():
                #     rsp_datas = rsp_json["data"]["jobs"]
                # else:
                #     rsp_datas = [rsp_json["data"]]
                rsp_datas = [rsp_json["data"]]
        for rsp_data in rsp_datas:
            # kylin , ke3 v2: build refresh,batch build
            if "job_status" in rsp_data.keys() and "exec_start_time" in rsp_data.keys():
                job_ids.append(rsp_data["uuid"])
            # ke 4
            # elif "job_id" in rsp_data.keys():
            #     job_ids.append(rsp_data["job_id"])
        return job_ids

    def _get_jobs_status(self, job_ids):
        hook = HttpHook(method="GET", http_conn_id=self.conn_id)
        headers = {"Content-Type": "application/json;charset=utf-8"}
        if not self.is_kylin:
            headers.update({"Accept": "application/vnd.apache.kylin-v2+json",
                            "Accept - Language": "en"})
        job_status = []
        max_attempts = 3
        for job_id in job_ids:
            endpoint = "/kylin/api/jobs/{job_id}".format(job_id=job_id)
            attempts = 0
            is_success = False
            response = None
            while attempts < max_attempts and not is_success:
                try:
                    response = hook.run(endpoint=endpoint)
                    is_success = True
                except AirflowException as ae:
                    attempts += 1
                    if attempts >= max_attempts:
                        raise ae
                    else:
                        time.sleep(30)
            if response:
                job_status.append(response.json()["job_status"])

        return job_status

    def _is_jobs_end(self, jobs_status):
        end_num = 0
        for job_status in jobs_status:
            if job_status in ["FINISHED", "ERROR", "DISCARDED", "KILLED", "SUICIDAL", "STOPPED"]:
                end_num += 1
        if end_num == len(jobs_status):
            return True
        else:
            return False

    def _is_jobs_wrong(self, jobs_status):
        for job_status in jobs_status:
            if job_status in ["ERROR", "DISCARDED", "KILLED", "SUICIDAL", "STOPPED"]:
                return True
        else:
            return False

    def _track_job_completed(self, job_ids, sleep_time=60, max_process_duration=1600000000):
        if len(job_ids) == 0:
            return
        start_time = time.time()
        jobs_status = self._get_jobs_status(job_ids)
        self.log.info("job status: {jobs_status}".format(jobs_status=jobs_status))
        # NEW, PENDING, RUNNING, FINISHED, ERROR, DISCARDED, WAITING, KILLED, STOPPED, SUICIDAL;
        while not self._is_jobs_end(jobs_status):
            time.sleep(sleep_time)
            jobs_status = self._get_jobs_status(job_ids)
            self.log.info("job status: {jobs_status}".format(jobs_status=jobs_status))
            if time.time() - start_time > max_process_duration:
                raise AirflowException("kylin job {job_ids}  timeout!!!".format(job_ids=job_ids))

        # jobs status; note: if jobs_status list is empty will return True
        if self._is_jobs_wrong(jobs_status):
            raise AirflowException(
                "kylin job {job_ids} status:{job_status}".format(job_ids=job_ids, job_status=jobs_status))

    def run(self):
        if self.is_kylin:
            self._gen_apache_kylin_request()
        elif not self.is_open_source:
            self._gen_kyligence_request()
        self._gen_self_http_hook()
        self.log.info("method : {method}".format(method=self.method))
        self.log.info("endpoint : {endpoint}".format(endpoint=self.endpoint))
        self.log.info("data : {data}".format(data=self.data))
        self.log.info("headers : {headers}".format(headers=self.headers))
        response = self.hook.run(endpoint=self.endpoint, data=json.dumps(self.data), headers=self.headers)
        if self.track_job_status:
            job_ids = self._get_job_ids_from_response(response.json())
            self.log.info("jobs : {job_ids}".format(job_ids=job_ids))
            self._track_job_completed(job_ids, sleep_time=self.sleep_time, max_process_duration=self.time_out)
            self.log.info("---------------jobs  end-----------------")
        if self.op_mod == "query" and self.query_result_xcom_push:
            return response.json()
