import logging.config
import os
from enum import Enum
import avro.schema
import yaml
from avro.io import DatumWriter

MODE_ENV_VAR = "CONFIG_MODE"

USE_COLOR = False


class RunningMode(Enum):
    DEV = 'dev'
    TEST = 'test'
    PROD = 'prod'


class ConfigImportError(Exception):
    pass


class SectionSettings:
    def __init__(self, name):
        self.section_name = name

    def __getattr__(self, name):
        pass

    def __setattr__(self, name, value):
        super().__setattr__(name, value)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Settings(metaclass=Singleton):
    def __init__(self, running_mode=None):
        # Get running mode from environment variable
        self._mode = running_mode or os.environ.get(MODE_ENV_VAR, 'default')

        config_resources_path = os.path.dirname(__file__)

        if not self._mode or self._mode not in ["default", "dev", "test", "prod"]:
            raise ConfigImportError(f"Failed to load config, environment variable {MODE_ENV_VAR} is missing")

        # Logging config
        with open(f'{config_resources_path}/resources/logging/logging-{self._mode}.yml', 'r') as yml_file:
            cfg = yaml.load(yml_file)

        # Start use global logger
        try:
            cfg['handlers']['syslog']['app_name'] = os.environ.get('APP_NAME', 'windows')
        except Exception as err:
            print(err)
        logging.config.dictConfig(cfg)
        self._logger = logging.getLogger(__name__)
        self._logger.info(f'Logger {self._logger.name} loaded successfully')

        with open(f'{config_resources_path}/resources/config/config-default.yml', 'r') as yml_file:
            default_cfg_from_yml = yaml.load(yml_file)

        avro_schema = avro.schema.parse(open(f"{config_resources_path}/sample_schema.avsc", "rb").read())
        self.datum = DatumWriter(avro_schema)

        self.sections = set(default_cfg_from_yml.keys())
        for section_name in default_cfg_from_yml:
            section_obj = self.dict_to_section(section_name, dict(default_cfg_from_yml[section_name].items()))
            setattr(self, section_name, section_obj)

        with open(f'{config_resources_path}/resources/config/config-{self._mode}.yml', 'r') as yml_file:
            env_cfg_from_yml = yaml.load(yml_file)

        self._explicit_settings = set()
        if env_cfg_from_yml is None:
            return

        for section_name in env_cfg_from_yml:
            if section_name not in self.sections:
                raise RuntimeError("New sections must be define in default config first")
            section_obj = self.dict_to_section(section_name, dict(env_cfg_from_yml[section_name].items()), True)
            setattr(self, section_name, section_obj)

    def dict_to_section(self, section_name, data, mark_override=False):
        if data is None or not isinstance(data, dict):
            return
        section_obj = SectionSettings(name=section_name)
        for key, val in data.items():
            if isinstance(val, dict):
                val = self.dict_to_section(f'{section_name}/{key}', val)
            setattr(section_obj, key, val)
            if mark_override:
                self._explicit_settings.add(key)
        return section_obj

    def is_overridden(self, setting):
        return setting in self._explicit_settings

    def __repr__(self):
        return f'<{self.__name__} {self._mode}>'

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return


settings = Settings(running_mode='default')

"""
 Load and overwrite settings from Env variables
"""
settings.dao.source = os.environ.get('DAO_SOURCE', settings.dao.source)
settings.webapi.api_host = os.environ.get('WEBAPI_HOST', settings.webapi.api_host)
settings.webapi.api_port = os.environ.get('WEBAPI_PORT', settings.webapi.api_port)
settings.pi.host = os.environ.get('PI_SERVER', settings.pi.host)
settings.pi.port = os.environ.get('PI_PORT', settings.pi.port)
settings.pi.authentication_mode = os.environ.get('PI_AUTH_MODE', settings.pi.authentication_mode)
settings.pi.network_credentials_username = os.environ.get('PI_USER', settings.pi.network_credentials_username)
settings.pi.network_credentials_password = os.environ.get('PI_PW', settings.pi.network_credentials_password)
settings.piwebapi.verify_ssl = os.environ.get('VERIFY_SSL') == 'True'
settings.piwebapi.snapshot_updates_method = os.environ.get('SNAPSHOT_METHOD', settings.piwebapi.snapshot_updates_method)
settings.piwebapi.max_ids_in_url = os.environ.get('MAX_IDS_IN_URL', settings.piwebapi.max_ids_in_url)
settings.piwebapi.data_server_name = os.environ.get('DATA_SERVER_NAME', settings.piwebapi.data_server_name)
settings.piwebapi.max_count_allowed = os.environ.get('MAX_COUNT_ALLOWED', settings.piwebapi.max_count_allowed)
settings.data.buffer_size = os.environ.get('WS_BUFFER_SIZE', settings.data.buffer_size)
settings.packers.cid_ts_val_err_pack = os.environ.get('PI_SAMPLE_STRUCT', settings.packers.cid_ts_val_err_pack)
settings.packers.avro = os.environ.get('USE_AVRO_PACK', settings.packers.avro)
settings.pi_data.time_between_snapshot_events_poll = os.environ.get('TIME_BETWEEN_SNAPSHOT_EVENTS_POLL', settings.pi_data.time_between_snapshot_events_poll)