import sys
import logging
import pyodbc
import importlib

from pyhocon import ConfigFactory, ConfigTree
from typing import Iterator, Union, Dict, Any
from databuilder.extractor.base_extractor import Extractor


LOGGER = logging.getLogger(__name__)


class DremioMetadataExtractor(Extractor):

    '''
    Requirements:
        pyodbc & Dremio driver
    '''

    DREMIO_USER_KEY = 'user_key'
    DREMIO_PASSWORD_KEY = 'password_key'
    DREMIO_HOST_KEY = 'host_key'
    DREMIO_PORT_KEY = 'port_key'
    DREMIO_DRIVER_KEY = 'driver_key'
    MODEL_CLASS = 'model_class'
    SQL_STATEMENT = 'sql_statement'

    DEFAULT_AUTH_USER = 'dremio_auth_user'
    DEFAULT_AUTH_PW = 'dremio_auth_pw'
    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = '31010'
    DEFAULT_DRIVER = 'DSN=Dremio Connector'
    DEFAULT_MODEL_CLASS = None
    DEFAULT_SQL_STATEMENT = None

    DEFAULT_CONFIG = ConfigFactory.from_dict({
        DREMIO_USER_KEY: DEFAULT_AUTH_USER,
        DREMIO_PASSWORD_KEY: DEFAULT_AUTH_PW,
        DREMIO_HOST_KEY: DEFAULT_HOST,
        DREMIO_PORT_KEY: DEFAULT_PORT,
        DREMIO_DRIVER_KEY: DEFAULT_DRIVER,
        MODEL_CLASS: DEFAULT_MODEL_CLASS,
        SQL_STATEMENT: DEFAULT_SQL_STATEMENT
    })

    def init(self, conf: ConfigTree) -> None:
        conf = conf.with_fallback(DremioMetadataExtractor.DEFAULT_CONFIG)
        self.__sql_stmt = conf.get_string(DremioMetadataExtractor.SQL_STATEMENT)
        self.__model_class = self.__get_model_class(conf.get(DremioMetadataExtractor.MODEL_CLASS, None))
        
        driver = conf.get_string(DremioMetadataExtractor.DREMIO_DRIVER_KEY)
        
        if sys.platform == 'linux':
            driver = f'DRIVER={driver}'
        
        self.__dremio_odbc_cursor = pyodbc.connect(
            driver,
            uid=conf.get_string(DremioMetadataExtractor.DREMIO_USER_KEY),
            pwd=conf.get_string(DremioMetadataExtractor.DREMIO_PASSWORD_KEY),
            host=conf.get_string(DremioMetadataExtractor.DREMIO_HOST_KEY),
            port=conf.get_string(DremioMetadataExtractor.DREMIO_PORT_KEY),
            autocommit=True).cursor()
        self._extract_iter: Union[None, Iterator] = None

    def extract(self) -> Any:
        if not self._extract_iter:
            self._extract_iter = self.__get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return 'extractor.dremio'

    def __get_model_class(self, model_class_name):
        if model_class_name:
            module_name, class_name = model_class_name.rsplit(".", 1)
            mod = importlib.import_module(module_name)
            return getattr(mod, class_name)

    def __get_extract_iter(self) -> Any:
        LOGGER.info('SQL for Dremio metadata: {}'.format(self.__sql_stmt))
        for record in self.__dremio_odbc_cursor.execute(self.__sql_stmt):
            result = dict(zip([c[0] for c in self.__dremio_odbc_cursor.description], record))
            yield self.__model_class(**result) if self.__model_class else result
