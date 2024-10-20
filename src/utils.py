from dotenv import load_dotenv
from typing import Tuple
import mariadb as mdb
import os
import pandas as pd

load_dotenv(override=True)


# TODO-1: MySQL connection Method
def connect_to_mysql(host: str,
                     user: str,
                     port: int,
                     database: str = None):
    """ Connection to MariaDB Server

    Args:
        host (str, optional): Server Address. Defaults to 'localhost'.
        user (str, optional): Username. Defaults to 'root'.
        port (int, optional): port Number. Defaults to 3306.
        database (str, optional): Database Name to connect at login. Defaults to None.

    Returns:
        Tuple(connection string, cursor)
    """

    try:
        connection = mdb.connect(host=host, user=user, port=port, password=os.environ['SERVER_PASSWORD'], database=database)
        cursor = connection.cursor()
        return connection, cursor
    except mdb.Error as conn_err:
        raise(conn_err)


# TODO-2: Database Creation Method
def create_database(cursor, dbname: str) -> bool:
    """ Drop database if exists and create new one

    Args:
        conn (sql.connection.MySQLConnection):  connection string as argument
        cursor (sql.connection.MySQLCursor):  cursor string as argument

    Returns:
        bool: returns True/False if query executed successfully or not
    """
    
    try:
        query_drop_database = f"DROP DATABASE IF EXISTS {dbname};"
        query_create_database = f"CREATE DATABASE {dbname};"
        cursor.execute(query_drop_database)
        cursor.execute(query_create_database)
        print("Database Created")
        cursor.execute("SHOW DATABASES;")  
        databases = cursor.fetchall()  
        return databases  

    except mdb.Error as db_create_err:
        raise(db_create_err)


# TODO-3: Table Creation Method
def create_table(cursor,
                 database:str,
                 tbname:str,
                 col_type:str) -> bool:
    
    """ Drops table if exists and create new one with provided parameters for columns

    Args:
        cursor (sql.connection.MySQLCursor):  cursor string as argument
        database (str): database name to insert table 
        tbname (str): table name for creating table
        col_type (str): columns with its data types

    Returns:
        bool: returns True/False if query executed successfully or not
    """
    
    try:
        query_db = f"USE {database};"
        query_drop_table = f"DROP TABLE IF EXISTS {tbname};"
        query_create_table = f"CREATE TABLE {tbname}({col_type})"
        cursor.execute(query_db)
        cursor.execute(query_drop_table)
        cursor.execute(query_create_table)
        print("Table Created")
        return True
    except mdb.Error as tb_create_err:
        raise(tb_create_err)


# TODO-4: Inserting Data to Table
# Another task; find what datatypes for data will be passed as argument (list, string, dictionary, tuple)
def insert_data_to_table(conn, 
                         cursor, 
                         database:str, 
                         table:str,
                         total_field:str,
                         col_names: str,
                         dataframe:pd.DataFrame) -> int:
    """Insert data to given table from the database provided as an argument

    Args:
        conn (sql.connection.MySQLConnection:  connection string as argument
        cursor (sql.connection.MySQLCursor):  cursor string as argument
        database (str): database name to insert table 
        table (str): table name to which the data will be inserted
        total_field (str): total fields for columns
        col_names (str): string literal contains name of columns
        dataframe (pd.DataFrame): pandas dataframe which holds data
    Returns:
        bool: returns True/False if query executed successfully or not
    """

    row_inserted = 0
    for _, row in dataframe.iterrows():
        try:
            ins_qry = f"INSERT INTO {database}.{table}({col_names}) VALUES ({total_field})"
            cursor.execute(ins_qry, tuple(row))
            if cursor.rowcount == 1:
                row_inserted += 1
                conn.commit()
        except mdb.Error as insert_error:
            print(insert_error, " at ", row)
            continue
    return row_inserted


# TODO-5: Fetch Data Method
def get_data_from_table(cursor, database:str, query: str) -> list:
    """ Pull data from the table in the database passed in argument

    Args:
        cursor (sql.connection.MySQLCursor):  cursor string as argument
        database (str): database name where table is located
        query (Str): sql query to fetch data
    Returns:
        list: returns retrived data in form of list
    """
    try:
        query_use_db = f"USE {database};"
        
        cursor.execute(query_use_db)
        cursor.execute(query)
        return cursor.fetchall()
    except mdb.Error as pull_data_error:
        raise(pull_data_error)


# TODO-6: Get Data From CSV
def get_data_from_csv(fname:str) -> pd.DataFrame:
    """Reading csv file and returning Dataframe

    Args:
        fname (str): location where csv file is located

    Returns:
        _type_: return pandas datatype
    """
    df = pd.read_csv(fname)
    df.drop(['Unnamed: 0'], axis=1, inplace=True, errors='ignore')
    return df

# TODO-7: Create Schema
def schema_template(dataframe) -> Tuple[str, str]:
    """ Create general template for rows to be inserted in table

    Args:
        dataframe (_type_): pass pandas Dataframe loaded with data

    Returns:
        _type_: column data types and total fields in form of string Tuple
    """
    types = ""
    col_names = col_names = ', '.join([dataframe.columns[i] for i in range(len(dataframe.columns))])

    for i, col_type in enumerate(dataframe.dtypes):
        col_name = dataframe.columns[i]
        col_name = col_name.replace('.', '_')
        if col_type == 'object':
            types += f'{col_name} VARCHAR(255), '
        elif col_type == 'float64':
            types += f'{col_name} DECIMAL(6,2), '
        elif col_type == 'int64':
            types += f'{col_name} INT, '
    col_datatypes = types[:-2]
    total_fields = ', '.join(len(dataframe.columns) * ['%s'])
    return col_names, col_datatypes, total_fields
