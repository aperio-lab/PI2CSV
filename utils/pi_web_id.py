import base64
import re
import binascii
from utils.singleton import Singleton
from utils.log import logger_class


class PIWebIdHelper(metaclass=Singleton):

    _data_server_web_id = None

    def __init__(self, data_server_web_id, *args, **kwargs):
        self._data_server_web_id = data_server_web_id[4:]
        self._logger = logger_class(self)
        self._web_id_to_id_map = dict()

    @staticmethod
    def _strip(guid: str):
        guid = re.sub(r'=', '', guid)
        guid = re.sub(r'/', '_', guid)
        return re.sub(r'\+', '-', guid)

    @staticmethod
    def get_owner_marker(data_type='PIPoint'):
        marker = {
            'PIPoint': 'DP',
            'PIServer': 'DS',
            'PISystem': 'RS'
        }
        return marker.get(data_type)

    def to_id(self, web_id):

        object_id = self._web_id_to_id_map.get(web_id)

        if object_id is None:
            try:
                object_id = int.from_bytes(base64.b64decode((web_id[-6:] + '==').replace('-', '+')), byteorder='little')
            except binascii.Error as err:
                self._logger.debug(f'Failed to convert {web_id} to id')
                self._logger.exception(err)

        return int(object_id)

    def find_id(self, pi_points: list, web_id: str):
        if len(pi_points) == 0:
            raise Exception('pi_points must be a list with at least one point')
        try:
            point = next(item for item in pi_points if item.web_id == web_id)
            if point is not None:
                return int(point.id)
        except StopIteration:
            self._logger.debug(f'Unable to find {web_id} inside pi_points, moving to base64decode strategy')
            return self.to_id(web_id)

    def to_web_id(self, object_id, data_server_web_id=None, data_type='PIPoint'):
        web_id = None

        # Try to cast object_id to int
        # if Value error is raised, object_id is probably already web_id
        try:
            int(object_id)
        except ValueError:
            return object_id

        # get the marker for the datatype
        marker = self.get_owner_marker(data_type)

        # get the relevant portion out of data_server_web_id, first 4 bytes represent the web_id_type and data_type
        serverwebid = data_server_web_id[4:] if data_server_web_id else self._data_server_web_id

        # return webid if datatype is a pi point
        if data_type == "PIPoint":
            if not object_id:
                raise Exception('provide a valid PI Point ID')
            arr = [int(i) for i in int(object_id).to_bytes(4, byteorder='little', signed=False)]
            pointwebid = base64.b64encode(bytes(arr)).decode('utf-8')
            web_id = f'I1{marker}{serverwebid}{self._strip(pointwebid)}'

        # return webid if datatype is a server
        elif data_type in ["PIServer", "PISystem"]:
            web_id = f'I1{marker}{serverwebid}'

        # cache object_id
        if self._web_id_to_id_map.get(web_id) is None:
            self._web_id_to_id_map[web_id] = object_id

        return web_id
