from enum import Enum, auto


class DremioOdbcSqlQueryBuilder(object):

    '''
        A query builder class to create SQL query statements for Dremio ODBC metadata extractor
    '''

    '''
        An enum for supported SQL ODBC query statements
    '''
    class SqlStatement(Enum):
        TABLE_OWNER_INIT = auto()
        APPLICATION = auto()

    '''
        An enum for SQL WHERE statement options, to customize SQL data records filtering
    '''
    class SqlWhereStatement(Enum):
        EXCLUDE_PDS_TABLES = auto()
        EXCLUDE_SYS_TABLES = auto()
        EXCLUDE_PDS_AND_SYS_TABLES = auto()
        INCLUDE_ALL = auto()

    __TABLE_OWNER_INIT_STMT = '''
    SELECT
      TABLE_CATALOG AS db_name,
      TABLE_SCHEMA AS schema,
      '{cluster}' as cluster,
      TABLE_NAME AS table_name,
      '{owners}' AS owners
    FROM INFORMATION_SCHEMA."TABLES"
    {where_stmt};
    '''

    __APPLICATION_CONTEXT_INIT_STMT = '''
    SELECT
      '{task_id}' as task_id,
      '{dag_id}' as dag_id,
      '{exec_date}' as exec_date,
      '{application_url_template}' as application_url_template,
      TABLE_CATALOG AS db_name,
      '{cluster}' as cluster,
      TABLE_SCHEMA AS schema,
      TABLE_NAME AS table_name
    FROM INFORMATION_SCHEMA."TABLES"
    {where_stmt};
    '''

    __SQL_STMT_MAPPING = {SqlStatement.TABLE_OWNER_INIT: __TABLE_OWNER_INIT_STMT,
                          SqlStatement.APPLICATION: __APPLICATION_CONTEXT_INIT_STMT}

    __TABLE_TYPE_NOT_PDS_EXPR = "{prefix}TABLE_TYPE != 'TABLE'"
    __TABLE_TYPE_NOT_SYS_EXPR = "{prefix}TABLE_TYPE != 'SYSTEM_TABLE'"
    __WHERE_STMT_MAPPING = {SqlWhereStatement.EXCLUDE_PDS_TABLES: f'WHERE {__TABLE_TYPE_NOT_PDS_EXPR}',
                            SqlWhereStatement.EXCLUDE_SYS_TABLES: f'WHERE {__TABLE_TYPE_NOT_SYS_EXPR}',
                            SqlWhereStatement.EXCLUDE_PDS_AND_SYS_TABLES: 'WHERE %s and %s'% (__TABLE_TYPE_NOT_PDS_EXPR,
                                                                                              __TABLE_TYPE_NOT_SYS_EXPR),
                            SqlWhereStatement.INCLUDE_ALL: ''}

    '''
        A method to create SQL statements.
        Args:
            - sql_stmt_enum(SqlStatement) - one of supported Dremio SQL statement (see SqlStatement class definition for details)

            - where_stmt(Tuple(SqlWhereStatement, str) or just SqlWhereStatement) - specify SQL WHERE statement details.

                If where_stmt is a tuple of (SqlWhereStatement, str) then SqlWhereStatement specified exclusion type
                (see SqlWhereStatement for details) and str allows to customize a prefix used in WHERE statement (splitted by ".")
                based on sql_stmt_enum (see above).
                For example if prefix is set to "abc", the where_stmt looks like: "WHERE abc.TABLE_TYPE != <...>"
                
                If where_stmt is an instance of SqlWhereStatement class, then prefix is set to empty string by default.
                For example: "WHERE TABLE_TYPE != <...>

            - kwargs - dynamic parameters, which are passed to sql_stmt_enum
    '''
    @staticmethod
    def get_sql_statement(sql_stmt_enum, where_stmt=None, **kwargs):
        stmt_type, stmt_prefix = (DremioOdbcSqlQueryBuilder.SqlWhereStatement.INCLUDE_ALL, '')
        if where_stmt:
            stmt_type, stmt_prefix = where_stmt if type(where_stmt) is tuple else (where_stmt, '')
            stmt_prefix += '.' if stmt_prefix else ''
        query_kwargs = {**kwargs,
                        'where_stmt': DremioOdbcSqlQueryBuilder.__WHERE_STMT_MAPPING[stmt_type].format(prefix=stmt_prefix)}
        return DremioOdbcSqlQueryBuilder.__SQL_STMT_MAPPING[sql_stmt_enum].format(**query_kwargs)
