# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import sys
from collections import namedtuple
from itertools import groupby
import logging
from typing import Iterator, Union, Dict, Any

from pyhocon import ConfigFactory, ConfigTree
from pyodbc import connect

from databuilder.extractor.base_extractor import Extractor
from databuilder.models.table_metadata import TableMetadata, ColumnMetadata


TableKey = namedtuple('TableKey', ['schema', 'table_name'])

LOGGER = logging.getLogger(__name__)


class DremioTableColumnExtractor(Extractor):
    '''
    Extracts Dremio table and column metadata from underlying INFORMATION_SCHEMA table
    Requirements:
        pyodbc & Dremio driver
    '''

    SQL_STATEMENT = '''
    SELECT
      nested_1.COLUMN_NAME AS col_name,
      CAST(NULL AS VARCHAR) AS col_description,
      nested_1.DATA_TYPE AS col_type,
      nested_1.ORDINAL_POSITION AS col_sort_order,
      nested_1.TABLE_CATALOG AS database,
      '{cluster}' AS cluster,
      nested_1.TABLE_SCHEMA AS schema,
      nested_1.TABLE_NAME AS name,
      CASE WHEN nested_2.WIKI IS NULL THEN '' ELSE nested_2.WIKI END AS description,
      CASE WHEN nested_0.TABLE_TYPE='VIEW' THEN TRUE ELSE FALSE END AS is_view,
      CASE WHEN nested_2.TAGS IS NULL THEN '' ELSE nested_2.TAGS END AS tags
    FROM (
      SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
      FROM INFORMATION_SCHEMA."TABLES"
    ) nested_0
    RIGHT JOIN (
      SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
      FROM INFORMATION_SCHEMA."COLUMNS"
    ) nested_1 ON nested_0.TABLE_NAME = nested_1.TABLE_NAME
      AND nested_0.TABLE_SCHEMA = nested_1.TABLE_SCHEMA
      AND nested_0.TABLE_CATALOG = nested_1.TABLE_CATALOG
    LEFT JOIN (
      SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, WIKI, TAGS
      FROM {dremio_collaboration_metadata_table}
    ) nested_2 ON nested_0.TABLE_NAME = nested_2.TABLE_NAME
      AND nested_0.TABLE_SCHEMA = nested_2.TABLE_SCHEMA
      AND nested_0.TABLE_CATALOG = nested_2.TABLE_CATALOG
    {where_stmt}
    '''

    # Config keys
    DREMIO_USER_KEY = 'user_key'
    DREMIO_PASSWORD_KEY = 'password_key'
    DREMIO_HOST_KEY = 'host_key'
    DREMIO_PORT_KEY = 'port_key'
    DREMIO_DRIVER_KEY = 'driver_key'
    DREMIO_CLUSTER_KEY = 'cluster_key'
    DREMIO_EXCLUDE_SYS_TABLES_KEY = 'exclude_system_tables'
    DREMIO_EXCLUDE_PDS_TABLES_KEY = 'exclude_pds_tables'
    DREMIO_COLLABORATON_METADATA_TABLE_KEY = 'dremio_collaboration_metadata_table_key'
    DREMIO_TAGS_SEPARATOR = 'tags_seperator'

    # Default values
    DEFAULT_AUTH_USER = 'dremio_auth_user'
    DEFAULT_AUTH_PW = 'dremio_auth_pw'
    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = '31010'
    DEFAULT_DRIVER = 'DSN=Dremio Connector'
    DEFAULT_CLUSTER_NAME = 'Production'
    DEFAULT_EXCLUDE_SYS_TABLES = True
    DEFAULT_EXCLUDE_PDS_TABLES = False
    DEFAULT_DREMIO_COLLABORATON_METADATA_TABLE = 'DREMIO_COLLABORATION_METADATA'
    DEFAULT_TAGS_SEPARATOR = None

    # Default config
    DEFAULT_CONFIG = ConfigFactory.from_dict({
        DREMIO_USER_KEY: DEFAULT_AUTH_USER,
        DREMIO_PASSWORD_KEY: DEFAULT_AUTH_PW,
        DREMIO_HOST_KEY: DEFAULT_HOST,
        DREMIO_PORT_KEY: DEFAULT_PORT,
        DREMIO_DRIVER_KEY: DEFAULT_DRIVER,
        DREMIO_CLUSTER_KEY: DEFAULT_CLUSTER_NAME,
        DREMIO_EXCLUDE_SYS_TABLES_KEY: DEFAULT_EXCLUDE_SYS_TABLES,
        DREMIO_EXCLUDE_PDS_TABLES_KEY: DEFAULT_EXCLUDE_PDS_TABLES,
        DREMIO_COLLABORATON_METADATA_TABLE_KEY: DEFAULT_DREMIO_COLLABORATON_METADATA_TABLE,
        DREMIO_TAGS_SEPARATOR: DEFAULT_TAGS_SEPARATOR
    })

    def init(self, conf: ConfigTree) -> None:
        conf = conf.with_fallback(DremioTableColumnExtractor.DEFAULT_CONFIG)

        exclude_sys_tables = conf.get_bool(DremioTableColumnExtractor.DREMIO_EXCLUDE_SYS_TABLES_KEY)
        exclude_pds_tables = conf.get_bool(DremioTableColumnExtractor.DREMIO_EXCLUDE_PDS_TABLES_KEY)
        if exclude_sys_tables and exclude_pds_tables:
            where_stmt = ("WHERE nested_0.TABLE_TYPE != 'SYSTEM_TABLE' AND "
                          "nested_0.TABLE_TYPE != 'TABLE';")
        elif exclude_sys_tables:
            where_stmt = "WHERE nested_0.TABLE_TYPE != 'SYSTEM_TABLE';"
        elif exclude_pds_tables:
            where_stmt = "WHERE nested_0.TABLE_TYPE != 'TABLE';"
        else:
            where_stmt = ';'

        self._cluster = conf.get_string(DremioTableColumnExtractor.DREMIO_CLUSTER_KEY)
        self._tags_separator = conf.get_string(DremioTableColumnExtractor.DREMIO_TAGS_SEPARATOR)
        self._dremio_collaboration_metadata_table = conf.get_string(DremioTableColumnExtractor.DREMIO_COLLABORATON_METADATA_TABLE_KEY)

        self.sql_stmt = DremioTableColumnExtractor.SQL_STATEMENT.format(
            cluster=self._cluster,
            dremio_collaboration_metadata_table=self._dremio_collaboration_metadata_table,
            where_stmt=where_stmt
        )

        LOGGER.info('SQL for Dremio metadata: {}'.format(self.sql_stmt))
        
        driver = conf.get_string(DremioTableColumnExtractor.DREMIO_DRIVER_KEY)
        if sys.platform == 'linux':
            driver = f'DRIVER={driver}'

        self._pyodbc_cursor = connect(
            driver,
            uid=conf.get_string(DremioTableColumnExtractor.DREMIO_USER_KEY),
            pwd=conf.get_string(DremioTableColumnExtractor.DREMIO_PASSWORD_KEY),
            host=conf.get_string(DremioTableColumnExtractor.DREMIO_HOST_KEY),
            port=conf.get_string(DremioTableColumnExtractor.DREMIO_PORT_KEY),
            autocommit=True).cursor()

        self._extract_iter: Union[None, Iterator] = None

    def extract(self) -> Union[TableMetadata, None]:
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self) -> str:
        return 'extractor.dremiotablecolumn'

    def _get_extract_iter(self) -> Iterator[TableMetadata]:
        '''
        Using itertools.groupby and raw level iterator, it groups to table and yields TableMetadata
        :return:
        '''
        for _, group in groupby(self._get_raw_extract_iter(), self._get_table_key):
            columns = []

            for row in group:
                last_row = row
                columns.append(ColumnMetadata(
                               row['col_name'],
                               row['col_description'],
                               row['col_type'],
                               row['col_sort_order'])
                               )

            yield TableMetadata(last_row['database'],
                                last_row['cluster'],
                                last_row['schema'],
                                last_row['name'],
                                last_row['description'],
                                columns,
                                last_row['is_view'] == 'true',
                                last_row['tags'].split(self._tags_separator) if last_row['tags'] else None)

    def _get_raw_extract_iter(self) -> Iterator[Dict[str, Any]]:
        '''
        Provides iterator of result row from SQLAlchemy extractor
        :return:
        '''

        for row in self._pyodbc_cursor.execute(self.sql_stmt):
            yield dict(zip([c[0] for c in self._pyodbc_cursor.description], row))

    def _get_table_key(self, row: Dict[str, Any]) -> Union[TableKey, None]:
        '''
        Table key consists of schema and table name
        :param row:
        :return:
        '''
        if row:
            return TableKey(schema=row['schema'], table_name=row['name'])

        return None
