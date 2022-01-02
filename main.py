import collections
import pytz
import ciso8601
import aiohttp
import json
import logging
import asyncio
import time
import base64
import click
import csv
import os
from conf import settings
from datetime import datetime, timedelta
from aiostream import stream
from typing import List
from tzlocal import get_localzone
from pydash.collections import flat_map
from pydash.objects import pick, keys
from concurrent.futures._base import CancelledError
from osisoft.pidevclub.piwebapi.pi_web_api_client import PIWebApiClient
from osisoft.pidevclub.piwebapi.models.pi_point import PIPoint
from osisoft.pidevclub.piwebapi.models.pi_items_item_point import PIItemsItemPoint
from osisoft.pidevclub.piwebapi.models.pi_summary_value import PISummaryValue
from concurrent.futures.thread import ThreadPoolExecutor
from utils.decorators import safe, safe_async
from utils.log import logger_class
from utils.helpers import decode_header
from utils.singleton import Singleton
from utils.misc import try_cast_to_int
from utils.pi_web_id import PIWebIdHelper

PointRecord = collections.namedtuple('PointRecord', 'Timestamp Value')
PointRecordWithError = collections.namedtuple('PointRecord', 'Timestamp Value Good')

MAX_IDS_IN_URL = int(settings.piwebapi.max_ids_in_url)
WEB_ID_TYPE = settings.piwebapi.web_id_type
MAX_COUNT_ALLOWED = settings.piwebapi.max_count_allowed
MAX_HISTORY_PERIOD_FOR_ARCHIVE = settings.piwebapi.max_history_period_for_archive
MAX_HISTORY_DAYS_PERIOD_FOR_ARCHIVE = settings.piwebapi.max_history_days_period_for_archive
MAX_COUNT_PI_POINTS_SEARCH = 10000
DATA_SERVER_NAME = settings.piwebapi.data_server_name
TIME_BETWEEN_SNAPSHOT_EVENTS_POLL = int(settings.pi_data.time_between_snapshot_events_poll)
SNAPSHOT_COUNTER_CALC_INTERVAL = 60 * 2


default_pi_server_ip = settings.pi.host
default_pi_pwd = settings.pi.password
default_pi_user = settings.pi.user

import warnings
warnings.filterwarnings("ignore")


class BaseWebAPIWrapper(metaclass=Singleton):
    _executor = ThreadPoolExecutor(max_workers=50,
                                   thread_name_prefix='PI_WRAPPER_EXECUTOR')
    webapi_auth_token = None
    web_id_helper = None

    _server = None
    _web_api_client = None
    _data_server = None
    _data_servers = None
    _point_api = None
    _stream_api = None
    _stream_set_api = None
    _event_frame_api = None
    _asset_db_api = None
    _web_id_helper = None
    _asset_server_api = None
    _data_server_api = None

    _data_server_web_id = None
    _time_zone = None

    def __init__(self, *args, **kwargs):
        self._logger = logger_class(self)
        self._loop = kwargs.get('loop') or asyncio.get_event_loop()
        self.webapi_auth_token = kwargs.get('webapi_auth_token')
        self._time_zone = str(get_localzone())
        self._date_format = kwargs.get('date_format', 'UNIX')

    @staticmethod
    def should_get_more_records(records_len: int, limit: int):
        return records_len == int(MAX_COUNT_ALLOWED) < limit

    @classmethod
    def get_web_api_client(self, server=None):
        if not self._point_api:
            if not hasattr(self._server, 'web_api_client') and server:
                self._server = server
            self._web_api_client = self._server.get('web_api_client')
        return self._web_api_client

    @classmethod
    def get_point_api(self, server=None):
        if not self._point_api:
            if not hasattr(self._server, 'web_api_client') and server:
                self._server = server
            self._point_api = self._server.get('web_api_client').point
        return self._point_api

    @classmethod
    def get_data_server_api(self, server=None):
        if not self._point_api:
            if not hasattr(self._server, 'web_api_client') and server:
                self._server = server
            self._data_server_api = self._server.get('web_api_client').dataServer
        return self._data_server_api

    @classmethod
    def get_stream_set(self, server=None):
        if not self._stream_set_api:
            if not hasattr(self._server, 'web_api_client') and server:
                self._server = server
            self._stream_set_api = self._server.get('web_api_client').streamSet
        return self._stream_set_api

    @classmethod
    def get_stream(self, server=None):
        if not self._stream_api:
            if not hasattr(self._server, 'web_api_client') and server:
                self._server = server
            self._stream_api = self._server.get('web_api_client').stream
        return self._stream_api

    @classmethod
    def get_event_frame(self, server=None):
        if not self._event_frame_api:
            if not hasattr(self._server, 'web_api_client') and server:
                self._server = server
            self._event_frame_api = self._server.get('web_api_client').eventFrame
        return self._event_frame_api

    @classmethod
    def get_asset_server(self, server=None):
        if not self._asset_server_api:
            if not hasattr(self._server, 'web_api_client') and server:
                self._server = server
            self._asset_server_api = self._server.get('web_api_client', PIWebApiClient).assetServer
        return self._asset_server_api

    @classmethod
    def get_asset_db_api(self, server=None):
        if not self._asset_db_api:
            if not hasattr(self._server, 'web_api_client') and server:
                self._server = server
            self._asset_db_api = self._server.get('web_api_client', PIWebApiClient).assetDatabase
        return self._asset_db_api

    @classmethod
    def get_data_server_web_id(self, server=None):
        if server.get('data_server', None) is None:
            return None
        if not self._data_server_web_id or self._data_server_web_id != server.get('data_server').web_id:
            if not hasattr(self._server, 'data_server') and server:
                self._server = server
            self._data_server_web_id = self._server.get('data_server').web_id
        return self._data_server_web_id

    @classmethod
    def get_web_id_from_id(self, db_web_id, cid):
        web_id_helper = self.get_web_id_helper(web_id=db_web_id)
        return web_id_helper.to_web_id(object_id=cid)

    @classmethod
    def get_web_id_helper(self, server=None, web_id=None, **kwargs):
        if not self._web_id_helper or self._web_id_helper._data_server_web_id != web_id:
            self._web_id_helper = PIWebIdHelper(web_id, **kwargs) if web_id is not None else PIWebIdHelper(self.get_data_server_web_id(server), **kwargs)
        return self._web_id_helper

    @staticmethod
    def parse_limit(limit):
        return int(MAX_COUNT_ALLOWED) if int(limit) <= 0 else int(limit)


class ClientWrapper(BaseWebAPIWrapper):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._web_api_client: PIWebApiClient = None
        self._data_server = None
        self._data_servers = list()
        self._is_server_connected = False

    def is_server_connected(self):
        return self._is_server_connected

    def get_server(self):
        """
        This function combines the two instances pi_server and data_server and returns them as server_obj dict
        :return: dict of pi_server and data_server
        """
        return {'web_api_client': self._web_api_client, 'data_server': self._data_server}

    async def get_server_info(self):
        return {
            "DAO_SOURCE": "PI_WEB_API",
            "DATA_SERVERS": [{
                "name": data_server.name,
                "web_id": data_server.web_id,
                "version": data_server.server_version,
            } for data_server in self._data_servers]
        }

    def is_connected(self):
        return self._is_server_connected

    @safe_async
    async def connect(self, hostname=None, network_creds=(None, None), data_server=None):
        """
        connect pi server with given conenctions params. blocking afsdk call will run in executor
        :param hostname: str, specify requested pi server ip. None for default pi server
        :param network_creds: tuple(str, str), specify credentials for pi
        :return: pi server obj
        """
        self._logger.debug(f'Connecting to pi server {hostname}')

        if not hostname:
            data = 'Unable to connect to PI server without valid IP address'
            status = 400
        else:
            try:
                default_headers = {
                    'Accept': "*/*",
                    'get_asset_server_info': hostname,
                    'Accept-Encoding': "gzip, deflate",
                    'Connection': "keep-alive"
                }
                self._web_api_client = PIWebApiClient(f'https://{hostname}/piwebapi', useKerberos=False, username=network_creds[0], password=network_creds[1], verifySsl=settings.piwebapi.verify_ssl)
                for header, value in default_headers.items():
                    self._web_api_client.api_client.set_default_header(header, value)
                data_servers = self._web_api_client.dataServer.list()

                if data_servers.items:
                    self._data_servers = data_servers.items
                    if data_server:
                        self._data_server = self._web_api_client.dataServer.get(web_id=data_server, web_id_type=WEB_ID_TYPE)
                    else:
                        data_server = DATA_SERVER_NAME or hostname
                        self._data_server = self._web_api_client.dataServer.get_by_name(name=data_server,
                                                                                        web_id_type=WEB_ID_TYPE)
                    data = await self.get_server_info()
                    status = 200
                else:
                    data = f'No data servers exists on PI server installed at {hostname}'
                    status = 400
            except Exception as err:
                self._logger.exception(err)
                data = err.error if hasattr(err, 'error') else str(err.args[0])
                if data_server is not None:
                    data['Data Server'] = data_server
                status = err.status if hasattr(err, 'status') else 503

            self._logger.debug(f'PI Server: {self._web_api_client}')

        return status == 200, {'data': data, 'status': status}

    # TODO: implement as property
    @safe_async
    async def disconnect(self):
        """
        disconnects the pi server.
        """
        await asyncio.sleep(0)

    @safe_async
    async def create_point(self, name, attr=None):
        """
        :param name: str. new point name
        :return: PIPoint. Newly created PI Point
        """
        self._logger.debug(f'Creating point, name: {name}')
        data_server_api = self.get_data_server_api(server=self.get_server())

        if not hasattr(attr, 'point_class'):
            attr['point_class'] = 'classic'
        if not hasattr(attr, 'point_type'):
            attr['point_type'] = 'Float32'

        try:
            await self._loop.run_in_executor(
                self._executor,
                data_server_api.create_point,
                self._data_server.web_id,
                PIPoint(name=name, **attr),
                WEB_ID_TYPE
            )
            return name

        except Exception as err:
            self._logger.exception(f'Failed to create {name} PIPoint: {err}')
            if 'Tag Already Exists in Table' in err.body:
                return name
            return None

    @safe_async
    async def create_points(self, points_names, point_source=None):
        """
        :param points_details: dict with point name and details, may need to convert to IDictionary
        :return: PIPoint. Newly created PI Point
        """
        self._logger.debug(f'Creating {len(points_names)} points')
        results = []
        for point_name in points_names:
            results.append(await self.create_point(point_name))
        return results

    async def delete_point(self, point):
        """
        :param point: dict. point to delete
        :return: delete point api call result
        """
        name = point.get('name')
        results = []
        self._logger.debug(f'Deleting point, name: {name}')
        results.append(await self._loop.run_in_executor(self._executor, self._data_server.delete, point.get('web_id')))
        return results

    @safe_async
    async def delete_points(self, points_names):
        """
        :param points_names as a list of point names
        :return: delete api call results
        """
        self._logger.debug(f'Deleting points: {points_names}')
        results = []
        for point_name in points_names:
            results.append(await self.delete_point(point_name))
        return results

    @safe_async
    async def find_point_changes(self, cookie):
        raise NotImplemented

    async def listen_to_point_changes(self):
        raise NotImplemented


class PointWrapper(BaseWebAPIWrapper):
    _point = None
    _attributes = {
        'web_id': 'WebId',
        'id': 'Id',
        'name': 'Name',
        'path': 'Path',
        'descriptor': 'Descriptor',
        'point_class': 'PointClass',
        'point_type': 'PointType',
        'digital_set_name': 'DigitalSetName',
        'span': 'Span',
        'zero': 'Zero',
        'engineering_units': 'EngineeringUnits',
        'step': 'Step',
        'future': 'Future',
        'display_digits': 'DisplayDigits',
        'web_exception': 'WebException',
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._logger.debug('Initiating snapshot_markers dictionary')
        self._point_ids = set()
        self.snapshot_markers = dict()

    def init_snapshot_data_pipes(self):
        self._logger.debug(f'Initiating snapshot update helpers')
        self._point_ids = set()

    async def close_snapshot_data_pipe(self):
        self._logger.debug(f'Closing snapshot pipe')
        self._point_ids.clear()

    async def set_point(self, server, **kwargs):
        """
        Notice point_id must be able to int casting
        :param data_server:
        :param args:
        :param kwargs: point_id: int
        :return:
        """
        if not self._server:
            self._server = server

        point = kwargs.get('point')
        point_id = kwargs.get('point_id')
        point_ids = kwargs.get('point_ids')

        if point:
            self._logger.debug(f'point: {point}, {type(point)}')
            if isinstance(point, PIPoint):
                self._point = point
            elif isinstance(point, int):
                self._point = await self.find_point_by_id(server, int(point))
            elif isinstance(point, str):
                self._point = await self.find_point_by_name(server, point)

        elif point_id:
            point_id = try_cast_to_int(point_id)
            if isinstance(point_id, str):
                self._point = PIPoint(web_id=point_id)
            elif isinstance(point_id, int):
                self._point = await self.find_point_by_id(server, point_id)

        elif point_ids:
            if self._point_ids is None:
                self._point_ids = set()
            for point in point_ids:
                self._point_ids.add(point)

    async def unset_points(self, pi_points: list):
        for point in pi_points:
            self._point_ids.discard(point)

    @safe
    def get_points(self):
        return list(self._point_ids)

    @safe
    async def get_point_web_ids(self, points=None):
        if self._server is None:
            raise Exception('Not connected to data server, please connected to data server first')

        self.web_id_helper = self.get_web_id_helper(self._server)
        try:
            return [pi_point.web_id for pi_point in points]
        except Exception as err:
            self._logger.error(err)
            return [self.web_id_helper.to_web_id(point)
                    for point in (points if points is not None else self.get_points())]

    @safe
    def get_point(self):
        return self._point

    @safe
    def get_point_name(self):
        return self._point.name

    async def get_point_metadata(self):
        raise NotImplemented

    @staticmethod
    async def find_point_by_name(server, name):
        if not isinstance(name, str):
            raise ValueError
        points = await PointWrapper().find_points(server=server, name_filter=name)
        if isinstance(points, list):
            return points[0]
        return points

    @staticmethod
    async def find_point_by_id(server, point_id):
        point_api = PointWrapper.get_point_api(server)
        point_id = try_cast_to_int(point_id)
        if isinstance(point_id, int):
            point_id = PointWrapper.get_web_id_helper(server).to_web_id(object_id=point_id)
        return await asyncio.get_event_loop().run_in_executor(None, lambda: point_api.get(web_id=point_id, web_id_type=WEB_ID_TYPE))

    @staticmethod
    async def find_points_by_id(server, points_id):
        point_api = PointWrapper.get_point_api(server)
        pi_points = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: point_api.get_multiple(web_id=points_id,
                                           as_parallel=True,
                                           include_mode='ItemsWithoutExceptionOnly',
                                           web_id_type=WEB_ID_TYPE).items
        )
        return [point.object for point in pi_points]

    @staticmethod
    async def find_points_by_web_id(server, point_web_ids: list):
        point_api = PointWrapper.get_point_api(server)
        if not isinstance(point_web_ids, list):
            raise ValueError
        pi_points = list()

        if len(point_web_ids) > MAX_IDS_IN_URL:
            for idx in range(0, len(point_web_ids), MAX_IDS_IN_URL):
                sub_points = point_web_ids[idx:idx + MAX_IDS_IN_URL]
                pi_points.extend(await PointWrapper.find_points_by_web_id(server, sub_points))
        else:
            pi_item_points = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: point_api.get_multiple(
                    web_id=point_web_ids,
                    web_id_type=WEB_ID_TYPE,
                    include_mode='ItemsWithoutExceptionOnly',
                    as_parallel=True
                )
            )
            if isinstance(pi_item_points, PIItemsItemPoint):
                if len(pi_item_points.items):
                    pi_points = [point.object for point in pi_item_points.items]

        return pi_points

    def load_metadata(self, pi_points):
        results = []
        t = tuple(keys(self._attributes))
        for pi_point in pi_points:
            res = pick(pi_point.to_dict(), *t)
            results.append(res)
        return results

    async def find_points(self, server, name_filter='*', source_filter='*'):
        data_server_api = server.get('web_api_client').dataServer
        return await self._loop \
            .run_in_executor(
                 None,
                 lambda: data_server_api.get_points(
                    web_id=self.get_data_server_web_id(server),
                    name_filter=name_filter,
                    web_id_type=WEB_ID_TYPE,
                    max_count=MAX_COUNT_PI_POINTS_SEARCH
                 ).items
            )

    async def get_points_by_name(self, server, name_list, attrs=None):
        results = []
        for name in name_list:
            results.append(await self.find_points(server, name))

        return flat_map(results)

    @staticmethod
    def get_limit_or_max(limit=MAX_COUNT_ALLOWED):
        max_count_allowed = int(MAX_COUNT_ALLOWED)
        if isinstance(limit, str):
            try:
                limit = int(limit)
            except ValueError:
                limit = 0
        return limit if 0 < limit <= max_count_allowed else max_count_allowed

    @staticmethod
    def convert_datetime_str_to_timestamp(datetimestr: str):
        # Convert date-time string format to float timestamp (epoch) format
        return ciso8601.parse_datetime(datetimestr).timestamp()

    async def get_point_plot_values(self, from_time, to_time, limit, tz=None):
        self._logger.debug(f'Get plot {self._point.web_id} values, from_time: {from_time}, to_time: {to_time}')
        try:
            if isinstance(from_time, str):
                from_time = float(from_time)
            if isinstance(to_time, str):
                to_time = float(to_time)
            if tz is None:
                tz = self._time_zone
        except ValueError as e:
            self._logger.exception(e)
            raise Exception('Unable to convert from_time/to_time string to float')
        limit = self.parse_limit(limit)
        stream = self.get_stream(self._server)

        try:
            records = await self._loop.run_in_executor(
                self._executor,
                lambda: stream.get_plot(
                    web_id=self._point.web_id,
                    start_time=datetime.fromtimestamp(from_time, pytz.timezone(tz)),
                    end_time=datetime.fromtimestamp(to_time, pytz.timezone(tz)),
                    intervals=self.get_limit_or_max(limit),
                    time_zone=tz
                ).items
            )
        except Exception as error:
            self._logger.exception(error)
            records = []

        for record in list(records):
            timestamp = self.convert_datetime_str_to_timestamp(record.timestamp)
            err = -1
            if isinstance(record.value, dict):
                err = record.value.get('Value')
                value = err
            else:
                value = float(record.value)
            yield self._point.web_id, timestamp, value, err

    async def get_point_values(self, from_time, to_time, limit=0, filter_expression=None):
        self._logger.debug(f'Get point {self._point.web_id} values, from_time: {from_time}, to_time: {to_time}')
        try:
            if isinstance(from_time, str):
                from_time = float(from_time)
            if isinstance(to_time, str):
                to_time = float(to_time)
        except ValueError as e:
            self._logger.exception(e)
            raise Exception('Unable to convert from_time/to_time string to float')

        limit = self.parse_limit(limit)
        stream = self.get_stream(self._server)

        try:
            records = await self._loop.run_in_executor(self._executor,
                lambda: stream.get_recorded(
                    web_id=self._point.web_id,
                    start_time=datetime.fromtimestamp(from_time),
                    end_time=datetime.fromtimestamp(to_time),
                    max_count=self.get_limit_or_max(limit),
                    time_zone=self._time_zone,
                    filter_expression=filter_expression,
                    include_filtered_values=True
                ).items
            )
        except Exception as error:
            self._logger.exception(error)
            records = []

        timestamp = None
        for record in list(records):
            timestamp = self.convert_datetime_str_to_timestamp(record.timestamp)
            err = -1
            if isinstance(record.value, dict):
                err = record.value.get('Value')
                value = err
            else:
                try:
                    value = float(record.value)
                except ValueError as e:
                    self._logger.exception(f'Failed to cast record value to float: {e}')
                    value = float(0)
            yield self._point.web_id, timestamp, value, err

        if self.should_get_more_records(len(records), int(limit)):
            async for record in self.get_point_values(from_time=timestamp + 1, to_time=to_time, limit=limit):
                yield record

    async def get_point_values_by_count(self, to_time, limit, forward):
        self._logger.debug(f'Entered get_points_values, params')

        forward = (forward == 'true')
        try:
            if isinstance(to_time, str):
                to_time = float(to_time)
        except ValueError as e:
            self._logger.exception(e)
            raise Exception('Unable to convert to_time string to float')
        limit = self.parse_limit(limit)
        stream = self.get_stream(self._server)
        end_time = datetime.fromtimestamp(to_time) + timedelta(days=MAX_HISTORY_DAYS_PERIOD_FOR_ARCHIVE) \
                    if forward else datetime.fromtimestamp(to_time) - timedelta(days=MAX_HISTORY_DAYS_PERIOD_FOR_ARCHIVE)
        try:
            records = await self._loop.run_in_executor(self._executor,
                lambda: stream.get_recorded(
                    web_id=self._point.web_id,
                    start_time=datetime.fromtimestamp(to_time),
                    end_time=end_time,
                    max_count=self.get_limit_or_max(limit),
                    time_zone=self._time_zone
                ).items
            )
        except Exception as error:
            self._logger.exception(error)
            records = []

        timestamp = None
        for record in list(records):
            timestamp = self.convert_datetime_str_to_timestamp(record.timestamp)
            if timestamp == to_time:
                continue
            err = settings.pi_data.valid_data_code
            if isinstance(record.value, dict):
                err = record.value.get('Value')
                value = err
            else:
                try:
                    value = float(record.value)
                except ValueError as e:
                    self._logger.exception(f'Failed to cast record value to float: {e}')
                    value = float(0)
            yield self._point.web_id, timestamp, value, err

        if self.should_get_more_records(len(records), int(limit)):
            async for record in self.get_point_values_by_count(to_time=timestamp - 1, limit=int(limit) - len(records), forward=forward):
                yield record

    async def list_signups(self, server):
        self._logger.debug(f'list_signups: Listing points for snapshot update')
        return await self._loop.run_in_executor(self._executor, self.get_points)

    async def get_registered_points(self, server):
        return self.load_metadata(await self.list_signups(server))

    async def get_registered_points_id(self, server):
        return await self.list_signups(server)

    async def add_to_signups_by_ids(self, server, pi_point_ids: List[str], **kwargs):
        self._logger.debug(f'add_to_signups_by_ids: adding {len(pi_point_ids)} points to snapshot update')
        await self.set_point(server, point_ids=pi_point_ids)
        await self.add_to_signups(server, pi_point_ids)

    async def set_latest_marker(self, item):
        if item.latest_marker is not None and item.latest_marker != '':
            self.snapshot_markers[item.source] = item.latest_marker
        elif item.status == 'MarkerNotFound':
            # marker has expired, get new marker for that channel
            self._logger.debug(f'set_latest_marker: Got {item.status} on {item.source}, getting new marker')
            await self.add_to_signups(self._server, [item.source], force_update=True)
        elif item.status == 'CacheNotFound':
            self._logger.debug(f'set_latest_marker: Got {item.status}')

            if item.requested_marker != '' and item.source == '':
                item.source = self.get_marker_source(item.requested_marker)
                if item.source is not None:
                    self._logger.debug(f'set_latest_marker: Getting new marker for {item.source} after hitting CacheNotFound')
                    await self.add_to_signups(self._server, [item.source], force_update=True)

            if item.source is None:
                # Need to re-register all tags if the "CacheNotFound" status comes for all tags.
                # Perhaps this is not the best way to do it, and we should try to re-register each tag separately,
                # but since we don't really know what is the source of the "CacheNotFound" marker, we may try to
                # re-register all the tags once again or restart pi-connector
                self.restart_snapshot_updates()

    def get_marker_source(self, marker):
        for web_id, latest_marker in self.snapshot_markers.items():  # for name, age in dictionary.iteritems():  (for Python 2.x)
            if latest_marker == marker:
                return web_id

    async def restart_snapshot_updates(self):
        self._logger.info(f'Restarting snapshot updates')
        self.snapshot_markers = dict()
        points_web_id = await self.get_registered_points_id(self._server)
        await self.add_to_signups(self._server, points_web_id, force_update=True)

    async def add_to_signups(self, server, pi_point_ids: List[PIPoint], **kwargs):
        stream_set_api = self.get_stream_set(server)
        self.web_id_helper = self.get_web_id_helper(server)
        point_web_ids = await self.get_point_web_ids(pi_point_ids)
        force_update = kwargs.get('force_update', False)

        self._logger.debug(f'add_to_signups: successfully retrieved '
                           f'{len(point_web_ids)} web_ids out of {len(pi_point_ids)} ids')

        """
        Split register to stream_set updates into into multiple requests, as each request
        may contain only limited number of channel web_ids in the URL before the URL became too long.
        Merge register results back to single list on finish.
        """
        pi_stream_register_items = list()

        for idx in range(0, len(point_web_ids), MAX_IDS_IN_URL):
            sub_points = point_web_ids[idx: idx + MAX_IDS_IN_URL]
            try:
                result = await self._loop.run_in_executor(
                    self._executor,
                    lambda: stream_set_api.register_stream_set_updates(
                        web_id=sub_points,
                        web_id_type=WEB_ID_TYPE
                    ).items
                )
            except Exception as err:
                self._logger.exception(err)
                result = []
            pi_stream_register_items.extend(result)
            await asyncio.sleep(0)

        """
        Set latest marker in the global snapshot_markers dict.
        This loop always sets the latest marker available, we may change it so the latest_marker will be only set,
        if no marker is already exists for that pi point.
        """
        snapshot_markers_keys = self.snapshot_markers.keys()
        for item in pi_stream_register_items:
            if item.source not in snapshot_markers_keys or force_update:
                await self.set_latest_marker(item)
        self._logger.debug(f'Now registered to {len(self.snapshot_markers.keys())} points for snapshot updates')

    async def remove_from_signups(self, pi_point_ids: list):
        self._logger.debug(f'remove from_signups: Removing {len(pi_point_ids)} points from snapshot update')
        await self.unset_points(pi_point_ids)
        for web_id in await self.get_point_web_ids():
            self.snapshot_markers.pop(web_id, None)

    async def remove_from_signups_by_id(self, server, pi_point_ids):
        await self.remove_from_signups(pi_point_ids)

    @staticmethod
    def convert_to_point_ilist(pi_points: List[PIPoint]):
        pi_points_ilist = list()
        for pi_point in pi_points:
            pi_points_ilist.append(pi_point)
        return pi_points_ilist

    def get_markers_list(self):
        return list(filter(None, self.snapshot_markers.values()))

    async def poll_snapshot_events(self, server, pi_points: list):
        """
       Poll PI web API for points snapshot updates
       This method distributes polling per small chunk of channels, and merges the events coming on stream
       :param server:
       :param pi_points:
       :return: events stream
       """
        if len(pi_points):
            await self.add_to_signups(server, pi_points)
        try:
            async for streamer in self.add_markers_to_queue(server):
                async for events in streamer:
                    yield events
        except CancelledError as err:
            self._logger.error(f'Sleep between snapshot events poll, unexpectedly cancelled with error: {err}')
        except StopAsyncIteration:
            self._logger.debug('Event stream complete')
        except RuntimeError:
            self._logger.debug('Event stream is dead')
        finally:
            self.init_snapshot_data_pipes()

    async def add_markers_to_queue(self, server):
        stream_set_api = self.get_stream_set(server)
        self._logger.info(f'Starting add_markers_to_queue periodic task')
        while True:
            markers_list = self.get_markers_list()
            markers_list_length = len(markers_list)
            if markers_list_length:
                #self._logger.info(f'Adding {markers_list_length} markers to retrieve stream-set updates queue')
                iterators_queue = [
                    self.retrieve_stream_set_updates(stream_set_api, markers_list[idx: idx + MAX_IDS_IN_URL])
                    for idx in range(0, markers_list_length, MAX_IDS_IN_URL)
                ]
                yield stream.merge(*iterators_queue).stream()
            self._logger.info('Going to sleep for %s seconds before next stream iteration' % TIME_BETWEEN_SNAPSHOT_EVENTS_POLL)
            await asyncio.sleep(TIME_BETWEEN_SNAPSHOT_EVENTS_POLL)

    async def retrieve_stream_set_updates(self, stream_set_api, latest_markers: list):
        snapshot_update_items = await self._loop.run_in_executor(
            self._executor,
            lambda: stream_set_api.retrieve_stream_set_updates(
                marker=latest_markers,
                web_id_type=WEB_ID_TYPE
            )
        )
        results = []
        for snapshot_item in snapshot_update_items.items:
            """
            Update local latest_markers dict with the latest marker
            """
            await self.set_latest_marker(snapshot_item)
            oid = snapshot_item.source
            name = snapshot_item.source_name
            for event in snapshot_item.events:
                if self._date_format == 'UNIX':
                    timestamp = self.convert_datetime_str_to_timestamp(event.timestamp)
                else:
                    timestamp = event.timestamp
                err = settings.pi_data.valid_data_code
                if isinstance(event.value, dict):
                    err = event.value.get('Value')
                    value = err
                    self._logger.debug(f'Got error value on snapshot event {err}')
                else:
                    try:
                        value = float(event.value)
                    except ValueError as e:
                        self._logger.exception(f'Failed to cast record value to float: {e}')
                        value = float(0)

                results.append((name, oid, timestamp, value, err))
        if len(results):
            self._logger.info(f'Received {len(results)} new events from PI')
        yield results

    async def get_snapshot_updates_over_ws(self, server, pi_point_ids: List[str]):
        rest_client = server.get('web_api_client').api_client.rest_client
        server_host = server.get('data_server').name

        self.web_id_helper = self.get_web_id_helper(server)

        web_ids_query_param = '&webId='.join(await self.get_point_web_ids(pi_point_ids))

        kwargs = {
            'headers': {
                'Content-Type': 'application/json',
                'Authorization': aiohttp.BasicAuth(
                                    login=rest_client.auth.username,
                                    password=rest_client.auth.password,
                                    encoding='utf-8').encode()
            },
            'verify_ssl': settings.piwebapi.verify_ssl
        }

        protocol = 'wss' if settings.piwebapi.verify_ssl else 'ws'
        url = f'{protocol}://{server_host}:443' \
              f'/piwebapi/streamsets/channel?webId={web_ids_query_param}&' \
              f'webIdType={WEB_ID_TYPE}&' \
              f'heartbeatRate={5}'
        self._logger.debug(f'{url}')

        session = aiohttp.ClientSession()

        self._logger.debug(f'Initiating WS connection with PI Web API. Url length: {len(url)}')
        async with session.ws_connect(url=url, **kwargs) as ws:
            try:

                async for message in ws:
                    if message.type == aiohttp.WSMsgType.BINARY:
                        self._logger.debug('WebSocket binary data received')
                    elif message.type == aiohttp.WSMsgType.CLOSED:
                        self._logger.debug('WebSocket Closed')
                        await session.close()
                    elif message.type == aiohttp.WSMsgType.ERROR:
                        self._logger.debug('WebSocket Error')
                        await session.close()
                    elif message.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(message.data)
                            items = data.get('Items', list())
                            results = []
                            for records in items:
                                oid = records.get('WebId')
                                for record in records.get('Items'):
                                    timestamp = self.convert_datetime_str_to_timestamp(record.get('Timestamp'))
                                    err = settings.pi_data.valid_data_code
                                    if isinstance(record.get('Value'), dict):
                                        err = record.get('Value').get('Value')
                                        value = err
                                    else:
                                        value = float(record.get('Value'))
                                    results.append((oid, timestamp, value, err))
                                await asyncio.sleep(0)
                            if len(results):
                                self._logger.debug(f'Received {len(results)} new events from PI')
                                yield results
                        except json.JSONDecodeError as err:
                            self._logger.exception(err)
            finally:
                await ws.close()
                await session.close()

    async def get_point_total_count(self, from_ts=0, to_ts=0):
        self._logger.debug('Get points total count is not implemented in PI Web API')
        yield []


class PointListWrapper(BaseWebAPIWrapper):

    _points = dict()
    _server = None

    def __init__(self, pi_points, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._server = kwargs.get('server')
        # _points is a dictionary where keys are web_ids and values are PIPoints
        self._points = {point.web_id: point for point in pi_points or kwargs.get('pi_points')}

    def get_points_web_ids(self):
        return list(self._points.keys())

    def get_id_by_web_id(self, web_id):
        try:
            oid = int(self._points[web_id].id)
        except KeyError as err:
            self._logger.debug(f'Unable to find id by web_id {web_id} inside self._points: {self._points}')
            oid = self.get_web_id_helper(self._server).to_id(web_id)

        return oid

    async def get_points_total_count(self, from_time, to_time):
        self._logger.debug('Get points total count')
        try:
            if isinstance(from_time, str):
                from_time = float(from_time)
            if isinstance(to_time, str):
                to_time = float(to_time)
        except ValueError as e:
            self._logger.exception(e)
            raise Exception('Unable to convert from_time/to_time string to float')

        stream_set = self.get_stream_set(self._server)

        try:
            records = await self._loop.run_in_executor(self._executor,
                   lambda: stream_set.get_summaries_ad_hoc(
                       web_id=self.get_points_web_ids(),
                       start_time=datetime.fromtimestamp(from_time) if from_time > 0 else f'-{MAX_HISTORY_PERIOD_FOR_ARCHIVE}',
                       end_time=datetime.fromtimestamp(to_time) if to_time > 0 else '*',
                       summary_type=['Count'],
                       web_id_type=WEB_ID_TYPE,
                       time_zone=self._time_zone
                   ).items
               )
        except Exception as error:
            self._logger.exception(error)
            records = []

        for record in list(records):
            err = settings.pi_data.valid_data_code
            value = 0
            if isinstance(record.items, list):
                if isinstance(record.items[0], PISummaryValue):
                    value = record.items[0].value.value
            elif isinstance(record.items, PISummaryValue):
                value = record.value.value
            yield record.web_id, value, err

    async def get_points_values(self, from_time, to_time, limit, timestamp_format='ts'):
        self._logger.debug(f'Entered get_points_values, params')

        try:
            if isinstance(from_time, str):
                from_time = float(from_time)
            if isinstance(to_time, str):
                to_time = float(to_time)
        except ValueError as e:
            self._logger.exception(e)
            raise Exception('Unable to convert from_time/to_time string to float')

        limit = self.parse_limit(limit)
        stream_set = self.get_stream_set(self._server)

        for records in await self._loop.run_in_executor(self._executor,
                   lambda: stream_set.get_recorded_ad_hoc(
                       web_id=self.get_points_web_ids(),
                       start_time=datetime.fromtimestamp(from_time),
                       end_time=datetime.fromtimestamp(to_time),
                       max_count=PointWrapper.get_limit_or_max(limit),
                       web_id_type=WEB_ID_TYPE,
                       time_zone=self._time_zone
                   ).items
               ):

            self._logger.debug(f'Received new page with {len(records.items)} records')
            oid = records.web_id
            timestamp = None
            for record in records.items:
                timestamp = PointWrapper.convert_datetime_str_to_timestamp(record.timestamp) if timestamp_format == 'ts' \
                    else record.timestamp
                err = settings.pi_data.valid_data_code
                if isinstance(record.value, dict):
                    err = record.value.get('Value')
                    value = err
                else:
                    try:
                        value = float(record.value)
                    except ValueError as e:
                        self._logger.exception(f'Failed to cast record value to float: {e}')
                        value = float(0)
                yield oid, timestamp, value, err
            await asyncio.sleep(0)

            if self.should_get_more_records(len(records.items), int(limit)):
                self._logger.debug(f'Getting more points values...')
                if isinstance(timestamp, str):
                    timestamp = PointWrapper.convert_datetime_str_to_timestamp(timestamp)
                async for record in self.get_points_values(from_time=timestamp + 1,
                                                           to_time=to_time,
                                                           limit=limit,
                                                           timestamp_format=timestamp_format):
                    yield record

    async def get_points_values_by_count(self, to_time, limit, forward, date_format='UNIX'):
        self._logger.debug(f'Entered get_points_values, params')
        forward = (forward == 'true')

        if isinstance(to_time, str):
            try:
                to_time = float(to_time)
            except ValueError as e:
                self._logger.exception(e)
                raise Exception('Unable to convert to_time to float')

        stream_set = self.get_stream_set(self._server)
        end_time = datetime.fromtimestamp(to_time) + timedelta(days=MAX_HISTORY_DAYS_PERIOD_FOR_ARCHIVE) \
            if forward else datetime.fromtimestamp(to_time) - timedelta(days=MAX_HISTORY_DAYS_PERIOD_FOR_ARCHIVE)

        for records in await self._loop.run_in_executor(self._executor,
                lambda: stream_set.get_recorded_ad_hoc(
                    web_id=self.get_points_web_ids(),
                    start_time=datetime.fromtimestamp(to_time),
                    end_time=end_time,
                    max_count=PointWrapper.get_limit_or_max(limit),
                    web_id_type=WEB_ID_TYPE,
                    sort_order='Ascending' if not forward else 'Descending',
                    time_zone=self._time_zone
                ).items
            ):
            self._logger.debug(f'Received new page with {len(records.items)} records')
            oid = records.web_id
            name = records.name
            timestamp = None
            for record in list(records.items):
                if self._date_format == 'UNIX':
                    timestamp = PointWrapper.convert_datetime_str_to_timestamp(record.timestamp)
                else:
                    timestamp = record.timestamp
                # if timestamp == to_time:
                #     continue
                err = settings.pi_data.valid_data_code
                if isinstance(record.value, dict):
                    err = record.value.get('Value')
                    value = err
                    self._logger.debug(f'Got error value on archive event: {err}')
                else:
                    try:
                        value = float(record.value)
                    except ValueError as e:
                        self._logger.exception(f'Failed to cast record value to float: {e}')
                        value = float(0)
                yield name, oid, timestamp, value, err

            if self.should_get_more_records(len(records.items), int(limit)):
                async for record in self.get_points_values_by_count(to_time=timestamp - 1,
                                                                    limit=int(limit) - len(records.items),
                                                                    forward=forward):
                    yield record

    async def get_point_plot_values(self, from_time, to_time, limit, tz=None):
        #self._logger.debug(f'Get plot {self._point.web_id} values, from_time: {from_time}, to_time: {to_time}')
        try:
            if isinstance(from_time, str):
                from_time = float(from_time)
            if isinstance(to_time, str):
                to_time = float(to_time)
            if tz is None and self._time_zone is not None:
                tz = self._time_zone
            else:
                tz = str(get_localzone())
        except ValueError as e:
            self._logger.exception(e)
            raise Exception('Unable to convert from_time/to_time string to float')
        limit = self.parse_limit(limit)
        stream_set = self.get_stream_set(self._server)

        try:
            records = await self._loop.run_in_executor(
                self._executor,
                lambda: stream_set.get_plot_ad_hoc(
                    web_id=self.get_points_web_ids(),
                    start_time=datetime.fromtimestamp(from_time, pytz.timezone(tz)),
                    end_time=datetime.fromtimestamp(to_time, pytz.timezone(tz)),
                    web_id_type=WEB_ID_TYPE,
                    intervals=limit,
                    time_zone=tz
                ).items
            )
        except Exception as error:
            self._logger.exception(error)
            records = []

        for streamValues in records:
            oid = streamValues.web_id
            for record in streamValues.items:
                timestamp = PointWrapper.convert_datetime_str_to_timestamp(record.timestamp)
                err = settings.pi_data.valid_data_code
                if isinstance(record.value, dict):
                    err = record.value.get('Value')
                    value = err
                else:
                    try:
                        value = float(record.value)
                    except ValueError as e:
                        self._logger.exception(f'Failed to cast record value to float: {e}')
                        value = float(0)
                yield oid, timestamp, value, err
            await asyncio.sleep(0)


class DataServer(metaclass=Singleton):
    # DAO = Data Access Object
    _data_server = None
    _pi_system = None
    _pi_system_wrapper = None
    _is_singleton = True

    connected = None
    _conn_status = dict()

    polling_snapshot_enable = False
    polling_changes_task = None

    def __init__(self, pi_config=None, pi_creds=(None, None), **kwargs):
        self._logger = logger_class(self)
        self._executor = kwargs.get('name') or ThreadPoolExecutor(thread_name_prefix='PI_WRAPPER_EXECUTOR')
        self._loop = kwargs.get('loop') or asyncio.get_event_loop()
        self._client_wrapper = ClientWrapper(**kwargs)
        self._is_singleton = not kwargs.get('not_singleton') is True or kwargs.get('singleton') is not False
        if not pi_config:
            self._logger.debug(f'pi_config was not provided as part of the request PI-Config header, '
                               f'setting _db_hostname from settings: {settings.pi.host}')
        self._db_hostname = kwargs.get('host') if kwargs.get('host') else settings.pi.host
        self._db_creds = (kwargs['user'], kwargs['password']) if kwargs.get('user') and kwargs.get('password') else (settings.pi.network_credentials_username, settings.pi.network_credentials_password)

    async def __aenter__(self):
        if not self._client_wrapper.is_connected():
            await self.connect_to_data_server()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client_wrapper.is_connected() and not self._is_singleton:
            await self.disconnect_from_data_server()
        elif self._is_singleton:
            self._logger.debug(f'This instance of DAO is singleton, therefore it will not disconnect from Data server on exit')

    async def connect_to_data_server(self, data_server=None):
        if self._data_server is None or not self.connected:
            self.connected = False
            self._conn_status = dict()
            retry = 0
            while not self.connected and retry < settings.pi.num_of_retries:
                retry = retry + 1
                self.connected, self._conn_status = await self._client_wrapper.connect(
                                                                      hostname=self._db_hostname,
                                                                      data_server=data_server,
                                                                      network_creds=self._db_creds)
            if self.connected:
                self._data_server = self._client_wrapper.get_server()
                self._logger.debug(f'Connected to data server: {self._data_server}')

        if self.connected:
            self._logger.debug(f'Already connected')
        else:
            self._logger.debug(f'Unable to connect to Data server, reason: {self._conn_status}')

        return self.connected

    async def disconnect_from_data_server(self, force=False):
        if not self._is_singleton or force is True:
            await self._client_wrapper.disconnect()

    async def reconnect_to_data_server(self):
        self._logger.debug('Re-connecting to Data Server...')
        self._data_server = None
        await self.disconnect_from_data_server(force=True)
        await self.connect_to_data_server()

    def get_conn_status(self):
        return self._conn_status

    def get_server_info(self):
        return self._client_wrapper.get_server_info()

    async def create_point(self, point_name, attr=None):
        return await self._client_wrapper.create_point(point_name, attr)

    async def create_points(self, points_names: list, attr=None):
        return await self._client_wrapper.create_points(points_names, attr)

    async def delete_point(self, point: dict):
        await self._client_wrapper.delete_point(point)

    async def set_point_wrapper(self, point, **kwargs):
        if not self._data_server and await self.connect_to_data_server() is not True:
            raise self.get_conn_status()
        pi_point_wrapper = PointWrapper(**kwargs)
        await pi_point_wrapper.set_point(self._data_server, point=point)
        return pi_point_wrapper

    async def write_point_value(self, point_id, timestamp, value):
        point_id = try_cast_to_int(point_id, logger=self._logger)
        try:
            pi_point_wrapper = await self.set_point_wrapper(point_id)
        except Exception as err:
            self._logger.exception(err)
            return err
        await pi_point_wrapper.update_point_value(PointRecord(point_id, timestamp, value))

    @staticmethod
    async def write_point_values(pi_point_wrapper, records):
        await pi_point_wrapper.update_point_values_with_errors(records)

    async def get_points_metadata(self, name_filter='*', source_filter='*', data_server_filter=None, asset_database_filter=None):
        if data_server_filter and isinstance(data_server_filter, list):
            data_server_filter = data_server_filter.pop()
        if not self._data_server and await self.connect_to_data_server(data_server_filter) is not True:
            return self.get_conn_status()

        pi_points_wrapper = PointWrapper(webapi_auth_token=self._client_wrapper.webapi_auth_token)

        if isinstance(name_filter, list):
            if len(name_filter) > 1:
                pi_points = await pi_points_wrapper.get_points_by_name(self._data_server, name_filter, source_filter[0])
            else:
                pi_points = await pi_points_wrapper.find_points(self._data_server, name_filter[0], source_filter[0])
        else:
            pi_points = await pi_points_wrapper.find_points(self._data_server, name_filter, source_filter)

        return await pi_points_wrapper.load_metadata(pi_points=pi_points)

    async def get_channel_archive(self, *args, **kwargs):
        cid = kwargs.get('channel_id')
        if not cid:
            raise RuntimeError('Channel ID parameter expected')
        if not self._data_server and await self.connect_to_data_server() is not True:
            yield self.get_conn_status()

        from_time = kwargs.get('from') or int(datetime.fromtimestamp(0).timestamp())
        to_time = kwargs.get('to') or int(time.time())
        pi_point_wrapper = PointWrapper(**kwargs)
        try:
            await pi_point_wrapper.set_point(self._data_server, point_id=cid)
        except Exception as err:
            self._logger.exception(err)
            yield {'data': err.body, 'status': 500}

        if kwargs.get('from') is None and \
           kwargs.get('to') is not None and kwargs.get('limit') is not None:
            # If from_time was not provided but limit does,
            # we assume that the request was made to get N samples of archive until to_time
            async for record in pi_point_wrapper.get_point_values_by_count(
                kwargs.get('to'),
                kwargs.get('limit', 1000),
                kwargs.get('forward', False)
            ):
                yield record
        else:
            async for record in pi_point_wrapper.get_point_values(
                    from_time, to_time, kwargs.get('limit', 0), kwargs.get('filter_expression', None)):
                yield record
        self._logger.debug(f'Yield keyword for archive done, params: {kwargs}')
        yield cid, time.time(), 0.0, settings.pi_data.end_of_archive_data_code

    async def get_channel_plot(self, *args, **kwargs):
        cid = kwargs.get('channel_id')
        if not cid:
            raise RuntimeError('Channel ID parameter expected')
        if not self._data_server and await self.connect_to_data_server() is not True:
            yield self.get_conn_status()

        from_time = kwargs.get('from') or int(datetime.fromtimestamp(0).timestamp())
        to_time = kwargs.get('to') or int(time.time())
        tz = kwargs.get('tz', None)
        pi_point_wrapper = PointWrapper(singleton=False)
        await pi_point_wrapper.set_point(self._data_server, point_id=cid)

        async for record in pi_point_wrapper.get_point_plot_values(from_time, to_time, kwargs.get('limit', 0), tz):
            yield record
        self._logger.debug(f'Yield keyword for archive done, params: {kwargs}')
        yield cid, time.time(), 0.0, settings.pi_data.end_of_archive_data_code

    async def get_multi_channel_archive(self, **kwargs):
        if not self._data_server and await self.connect_to_data_server() is not True:
            yield self.get_conn_status()

        if kwargs.get('from') is None and \
           kwargs.get('to') is not None and kwargs.get('limit') is not None:
            # If from_time was not provided but limit does,
            # we assume that the request was made to get N samples of archive until to_time
            async for record in self.get_multi_channel_archive_of_n_samples(**kwargs):
                yield record
        else:
            pi_points = await self.find_points_by_filter(**kwargs)
            from_time = kwargs.get('from') or 0
            to_time = kwargs.get('to') or datetime.now()
            timestamp_format = kwargs.get('timestamp_format', 'ts')
            converted_points = PointWrapper.convert_to_point_ilist(pi_points)
            pi_points_list_wrapper = PointListWrapper(converted_points, server=self._data_server, **kwargs)

            async for record in pi_points_list_wrapper.get_points_values(from_time, to_time, kwargs.get('limit', 0), timestamp_format):
                yield record

        self._logger.debug(f'Done multi channel get archive task, params: {kwargs}')

    async def get_multi_channel_archive_of_n_samples(self, **kwargs):
        if not self._data_server and await self.connect_to_data_server(kwargs.get('data_server')) is not True:
            yield self.get_conn_status()

        pi_points = await self.find_points_by_filter(**kwargs)
        converted_points = PointWrapper.convert_to_point_ilist(pi_points)
        pi_points_list_wrapper = PointListWrapper(converted_points, server=self._data_server, **kwargs)
        async for record in pi_points_list_wrapper.get_points_values_by_count(
                kwargs.get('to'),
                kwargs.get('limit', 1000),
                kwargs.get('forward', False),
                kwargs.get('date_format')
        ):
            yield record

        self._logger.debug(f'Done multi channel get archive by count task, params: {kwargs}')

    async def get_multi_channel_plot(self, **kwargs):
        if not self._data_server and await self.connect_to_data_server() is not True:
            yield self.get_conn_status()

        pi_points = await self.find_points_by_filter(**kwargs)
        from_time = kwargs.get('from') or 0
        to_time = kwargs.get('to') or datetime.now()
        tz = kwargs.get('tz') or None
        converted_points = PointWrapper.convert_to_point_ilist(pi_points)
        pi_points_list_wrapper = PointListWrapper(converted_points, server=self._data_server, **kwargs)
        async for record in pi_points_list_wrapper.get_point_plot_values(from_time, to_time, kwargs.get('limit', 0), tz):
            yield record

        self._logger.debug(f'Done multi channel get plot task, params: {kwargs}')

    async def get_multi_channel_snapshot(self, **kwargs):
        self._logger.info(f'Entered get multi channel snapshot task, params: {kwargs}')
        if not self._data_server and await self.connect_to_data_server(kwargs.get('data_server')) is not True:
            yield self.get_conn_status()
        pi_points = await self.find_points_by_filter(**kwargs)
        async for snapshot_records in await self.poll_snapshot_events(pi_points):
            for snapshot_record in snapshot_records:
                yield snapshot_record

    async def find_points_by_filter(self, *args, **kwargs):
        name_filter = kwargs.get('name_filter')
        points_list = kwargs.get('points_list', [])
        self._logger.debug(f'find_points_by_filter: {points_list}')
        pi_points = list()
        point_wrapper = PointWrapper(**kwargs)
        if points_list:
            if isinstance(points_list, str):
                points_list = points_list.split(',')
            pi_points.extend(await point_wrapper.find_points_by_id(self._data_server, points_list))
        if name_filter:
            query_result = await point_wrapper.find_points(self._data_server, name_filter)
            for pi_point in query_result:
                pi_points.append(pi_point)
        return pi_points

    async def list_subscribed_points(self):
        if not self._data_server and await self.connect_to_data_server() is not True:
            return self.get_conn_status()
        point_wrapper = PointWrapper()
        return await point_wrapper.get_registered_points_id(self._data_server)

    async def register_points_to_snapshot(self, **kwargs):
        if not self._data_server and await self.connect_to_data_server() is not True:
            return self.get_conn_status()
        pi_point_ids = kwargs.get('points_list', list())
        pi_point_wrapper = PointWrapper()

        await pi_point_wrapper.add_to_signups_by_ids(server=self._data_server, pi_point_ids=pi_point_ids, **kwargs)
        time.sleep(.5)
        return await self.list_subscribed_points()

    async def unregister_points_from_snapshot(self, **kwargs):
        if not self._data_server:
            raise RuntimeError
        pi_point_ids = kwargs.get('points_list', list())
        cid = kwargs.get('cid', None)

        pi_point_wrapper = PointWrapper()
        if cid is not None:
            pi_point = await pi_point_wrapper.set_point(self._data_server, point_id=cid)
            await pi_point_wrapper.remove_from_signups([pi_point])
        elif len(pi_point_ids) > 0:
            await pi_point_wrapper.remove_from_signups_by_id(self._data_server, pi_point_ids)
            time.sleep(.5)
        return await self.list_subscribed_points()

    async def poll_snapshot_events(self, points):
        if not self._data_server and await self.connect_to_data_server() is not True:
            return self.get_conn_status()
        pi_points_wrapper = PointWrapper()
        try:
            return pi_points_wrapper.poll_snapshot_events(server=self._data_server, pi_points=points)
        except Exception as e:
            pi_points_wrapper.init_snapshot_data_pipes()
            raise Exception(e)

    async def find_points(self, *args, **kwargs):
        if not self._data_server and await self.connect_to_data_server() is not True:
            return self.get_conn_status()
        return await PointWrapper.find_points(self._data_server, **kwargs)

    async def get_channel_count(self, *args, **kwargs):
        if not self._data_server and await self.connect_to_data_server() is not True:
            yield self.get_conn_status()
        from_time = kwargs.get('from') or 0
        to_time = kwargs.get('to') or datetime.now().timestamp()
        pi_point_wrapper = PointWrapper(**kwargs)
        await pi_point_wrapper.set_point(self._data_server, point_id=kwargs.get('channel_id'))
        async for records in pi_point_wrapper.get_point_total_count(from_time, to_time):
            yield records

    async def get_multi_channels_count(self, *args, **kwargs):
        if not self._data_server and await self.connect_to_data_server() is not True:
            yield self.get_conn_status()
        pi_points = await self.find_points_by_filter(**kwargs)
        from_time = kwargs.get('from') or 0
        to_time = kwargs.get('to') or datetime.now().timestamp()
        converted_points = PointWrapper.convert_to_point_ilist(pi_points)
        pi_points_wrapper = PointListWrapper(converted_points, server=self._data_server, **kwargs)
        async for records in pi_points_wrapper.get_points_total_count(from_time, to_time):
            yield records

    async def get_channel_web_id(self, channel_id, data_server_web_id=None):
        self._logger.info(f'Generating web_id for channel ID: {channel_id} and Data server {data_server_web_id}')
        if not data_server_web_id:
            if not self._data_server and await self.connect_to_data_server() is not True:
                return self.get_conn_status()
            pi_point_wrapper = PointWrapper()
            await pi_point_wrapper.set_point(self._data_server, point_ids=[channel_id])
            res = pi_point_wrapper.get_point_web_ids()
        else:
            res = PointWrapper.get_web_id_from_id(db_web_id=data_server_web_id, cid=channel_id)

        return res


class AuthService:
    def __init__(self, *args, **kwargs):
        headers = kwargs.get('headers')
        if headers:
            encoded_config_params = headers.get(settings.pi.header_name_config)
            encoded_creds_params = headers.get(settings.pi.header_name_creds)
            host, port = decode_header(encoded_config_params)
            user, password = decode_header(encoded_creds_params)
            kwargs['pi_config'] = host or settings.pi.host
            kwargs['pi_creds'] = (user, password) if user is not None and password is not None \
                else (settings.pi.network_credentials_username, settings.pi.network_credentials_password)
            kwargs['webapi_auth_token'] = headers.get('Authorization')
        self.kwargs = kwargs

    def get_kwargs(self):
        return self.kwargs


class BaseService(AuthService):

    def __init__(self, *args, **kwargs):

        kwargs = AuthService(**kwargs).get_kwargs()

        self.name = kwargs.get('name') or 'default'
        self._loop = kwargs.get('loop') or asyncio.get_event_loop()
        self._logger = logger_class(self)
        # DAO = Data Access Object
        try:
            self._data_server = DataServer(loop=self._loop, **kwargs)
            self._data_server_id = id(self._data_server)
        except Exception as err:
            self._logger.exception(err)

        self._logger.debug('DAU instance %s' % self._data_server_id)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._data_server.disconnect_from_data_server()

    async def test_pi_connection(self):
        status = await self._data_server.connect_to_data_server()
        if status:
            return status, await self._data_server.get_server_info()
        else:
            return status, self._data_server.get_conn_status()

    def get_archive(self, *args, **kwargs):
        raise NotImplementedError

    def get_snapshot(self, *args, **kwargs):
        raise NotImplementedError

    @staticmethod
    def db_settings_differ(data_server, **kwargs):
        return (kwargs.get('pi_config') is not None and kwargs.get('pi_config') != data_server._db_hostname) or (
                    kwargs.get('pi_creds') is not None and kwargs.get('pi_creds') != data_server._db_creds)


class MultiChannelDataService(BaseService):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await super().__aexit__(exc_type, exc_val, exc_tb)

    async def get_archive(self, *args, **kwargs):
        async for archive_record in self._data_server.get_multi_channel_archive(**kwargs):
            yield archive_record
        self._logger.debug(f'sending end_of_archive_data_code for channel {kwargs.get("points_list", "0")}')
        yield (kwargs.get('points_list', '0'), time.time(), float('nan'), settings.pi_data.end_of_archive_data_code)

    async def get_n_samples_archive(self, *args, **kwargs):
        async for archive_record in self._data_server.get_multi_channel_archive_of_n_samples(**kwargs):
            yield archive_record
        self._logger.debug(f'sending end_of_archive_data_code for channel {kwargs.get("points_list", "0")}')
        yield (kwargs.get('points_list', '0'), time.time(), float('nan'), settings.pi_data.end_of_archive_data_code)

    async def get_snapshot(self, *args, **kwargs):
        async for snapshot_record in self._data_server.get_multi_channel_snapshot(**kwargs):
            self._logger.debug(f'DAU #{self._data_server_id}: Snapshot record received: {snapshot_record}')
            yield snapshot_record

    async def get_plot(self, *args, **kwargs):
        async for plot_record in self._data_server.get_multi_channel_plot(**kwargs):
            yield plot_record

    async def get_count(self, *args, **kwargs):
        self._logger.debug('Get samples total count for %s' % kwargs.get('points_list', []))
        async for record in self._data_server.get_multi_channels_count(**kwargs):
            yield record

    async def list_subscribed_points(self):
        return await self._data_server.list_subscribed_points()

    async def subscribe_for_points_update(self, *args, **kwargs):
        # self._logger.debug('Subscribe for pi_points snapshot updates for %s' % kwargs.get('points_list', []))
        return await self._data_server.register_points_to_snapshot(**kwargs)

    async def unsubscribe_from_points_update(self, *args, **kwargs):
        # self._logger.debug('Unsubscribe from pi_points snapshot updates for %s' % kwargs.get('points_list', []))
        return await self._data_server.unregister_points_from_snapshot(**kwargs)


class PIDataToCSV:

    logger = None
    kwargs = None

    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__ + str(datetime.now().timestamp()))
        self.kwargs = kwargs

    async def main(self):

        data_type = self.kwargs.get('data_type')
        name_filter = self.kwargs.get('name_filter')
        data_server = self.kwargs.get('data_server')

        if not name_filter:
            raise Exception('No name_filter was specified for Tags search')
        if not data_server:
            connected, conn_info = await BaseService(**self.kwargs).test_pi_connection()
            if connected:
                self.logger.info('No data_server was specified for Tags search, therefor data-server was selected by the same hostname')
                [self.logger.info(server) for server in conn_info['DATA_SERVERS']]


        if data_type == 'n_samples_archive':
            await self.get_archive_data_for_tags()

        if data_type == 'snapshot':
            await self.get_snapshot_data_for_tags()

    async def get_async_data_source(self, data_type, **kwargs):

        async with MultiChannelDataService(**kwargs) as service:

            if data_type == 'archive':
                return service.get_archive(**kwargs)
            elif data_type == 'n_samples_archive':
                return service.get_n_samples_archive(**kwargs)
            elif data_type == 'snapshot':
                return service.get_snapshot(**kwargs)

    async def get_multi_channel_data(self, **kwargs):

        try:
            start_time = time.monotonic()
            data_type = kwargs.get('data_type')
            async_data_source = await self.get_async_data_source(singleton=False, **kwargs)
            data = []
            async for sample in async_data_source:
                if sample[-1] != settings.pi_data.valid_data_code:
                    continue
                data.append(sample)
            self.logger.info(f'Done handle get_multi_channel_data request: data_type={data_type}, in {(time.monotonic() - start_time)} seconds')

            return data

        except Exception as e:
            self.logger.exception('Failed to process request')

    async def stream_multi_channel_data_single_ws(self, **kwargs):

        try:
            data_type = kwargs.get('data_type')
            async_data_source = await self.get_async_data_source(
                                                            is_multi_channel=True,
                                                            singleton=True,
                                                            **kwargs)

            await self.write_buffer_to_csv(async_data_source)
            self.logger.info(f'Done handle multi channel request: data_type={data_type}')
        except Exception as e:
            self.logger.exception(e)

    async def write_buffer_to_csv(self, async_data_source, **kwargs):
        buffer = []
        stream_counter = dict()
        stream_start_time = time.monotonic()

        def init_file():
            kwargs['filename'] = os.path.dirname(os.path.realpath(__file__)) + \
                           f'/snapshot_export_{datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}.csv'

        init_file()

        async for sample in async_data_source:
            is_last_sample = sample[-1] in settings.pi_data.invalid_data_codes
            buffer.append(sample)
            stream_counter[sample[0]] = stream_counter.get(sample[0], 0) + 1

            if len(buffer) >= int(settings.data.buffer_size) or is_last_sample:
                self.logger.info('Flushing buffer to CSV')

                await self.write_data_2_csv(buffer)

                buffer = []
                if is_last_sample:
                    self.logger.info('EOS closing CSV')

                time_since_stream_start = time.monotonic() - stream_start_time

                if time_since_stream_start >= SNAPSHOT_COUNTER_CALC_INTERVAL:
                    init_file()
                    total_snapshot_received = 0
                    for key, value in stream_counter.items():
                        total_snapshot_received += value
                    self.logger.debug(f'Got {total_snapshot_received} samples on {len(stream_counter.items())} pi points from async stream_data')
                    stream_counter = dict()
                    stream_start_time = time.monotonic()

    async def get_archive_data_for_tags(self):

        data = await self.get_multi_channel_data(**self.kwargs)

        await self.write_data_2_csv(data)

    async def get_snapshot_data_for_tags(self):

        await self.stream_multi_channel_data_single_ws(**self.kwargs)

    async def write_data_2_csv(self, data):
        if not data:
            raise Exception('No data to export, make sure you\'ve provided all params correctly')

        if not self.kwargs.get('filename'):
            data_type = self.kwargs.get('data_type')
            filename = os.path.realpath(self.kwargs.get('export_dir') +
                       f'/{data_type}_export_{datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}.csv')
        else:
            filename = self.kwargs.get('filename')

        with open(filename, 'w', encoding='UTF8') as f:
            # create the csv writer
            writer = csv.writer(f)
            writer.writerow(['Tagname', 'Web_ID', 'Date Time', 'Value', 'Error'])
            # write a row to the csv file
            for row in data:
                writer.writerow(row)

            print('Done')


@click.command()
@click.option('--host', type=str, help='PI WEBAPI IP/Host address', default=default_pi_server_ip)
@click.option('--user', type=str, help='PI user', default=default_pi_user)
@click.option('--password', help='PI password', default=default_pi_pwd)
@click.option('--log', type=str, help='Set logger name', default=__name__)
@click.option('--limit', type=int, help='Num of last samples to get from PI archive for each tag', default=MAX_COUNT_ALLOWED)
@click.option('--to', type=int, help='Date-Time Timestamp to get archive data until, default is NOW', default=datetime.now().timestamp())
@click.option('--data_type', type=str, help='Type of data to get from PI: archive/snapshot', default='n_samples_archive')
@click.option('--name_filter', type=str, help='Tag name filter, use whilecard for pattern search', default=None)
@click.option('--data_server', type=str, help='PI Data Server web_id for tags search and data export', default=None)
@click.option('--date_format', type=str, help='Date format to use when writing time-series to CSV', default='%Y-%m-%d %H:%M:%S')
@click.option('--export_dir', type=str, help='Directory path for CSV export', default=os.path.realpath(os.path.dirname(__file__)))
def cli(*args, **kwargs):

    loop = asyncio.get_event_loop()
    pi2csv = PIDataToCSV(**kwargs)

    loop.run_until_complete(pi2csv.main())


if __name__ == '__main__':
    cli()
