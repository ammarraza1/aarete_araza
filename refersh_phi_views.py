
import streamlit
import pandas as pd
import snowflake.connector
import snowflake.snowpark as snowpark

ACCOUNT = "ty74833.us-east-2.aws"
ROLE = "SQL-US_DATA_ARCH"
WAREHOUSE = "CODE_DAE_WH"
SNF_SCHEMA = "DW"
SF_DB_NAME = "JHHC_DW"

streamlit.title("Refresh PHI views from DW tables")
streamlit.header("Refresh PHI views from DW tables")
streamlit.text("Refresh PHI views from DW tables")

def snowpark_session(account, role, warehouse, db_schema, db_name) -> Session:
    """
    Create Snowpark connection
    :param account:
    :param role:
    :param warehouse:
    :param db_schema:
    :param db_name:
    :return: Snowpark session
    """
    connection_parameters = {
        "account": account,
        "user": CODE_RDG_USERNAME,
        "password": base64.b64decode(CODR_RDG_ENCR_PWD).decode("utf-8"),
        "role": role,
        "warehouse": warehouse,
        "database": db_name,
        "schema": db_schema
    }
    return Session.builder.configs(connection_parameters).create()
  
session = snowpark_session(ACCOUNT, ROLE, WAREHOUSE, SNF_SCHEMA, SF_DB_NAME)

sql_statement = """
    CREATE OR REPLACE TEMP TABLE SUMMARY_COLUMN_CNT_TABLE_VIEWS
    AS
    SELECT DWTBL.TABLE_CATALOG, DWTBL.TABLE_NAME, DWTBL.DW_COLUMN_COUNT, PHI_COLUMN_COUNT, NON_PHI_COLUMN_COUNT
    FROM
    (
    Select table_catalog,table_schema , table_name, COUNT(DISTINCT COLUMN_NAME) AS DW_COLUMN_COUNT
    From snowflake.account_usage.columns
    Where table_schema = 'DW'
    Group BY TABLE_CATALOG,TABLE_SCHEMA , table_name) DWTBL
    INNER JOIN
    (Select table_catalog,table_schema , table_name, COUNT(DISTINCT COLUMN_NAME) AS PHI_COLUMN_COUNT
    From snowflake.account_usage.columns
    Where table_schema = 'PHI'
    Group BY TABLE_CATALOG,TABLE_SCHEMA , table_name) PHITBL
    ON DWTBL.table_catalog = PHITBL.table_catalog
    AND DWTBL.table_name=PHITBL.table_name
    inner join
    (
    Select table_catalog,table_schema , table_name, COUNT(DISTINCT COLUMN_NAME) AS NON_PHI_COLUMN_COUNT
    From snowflake.account_usage.columns
    Where table_schema = 'NON_PHI'
    Group BY TABLE_CATALOG,TABLE_SCHEMA , table_name) NONPHITBL
    ON DWTBL.table_catalog = NONPHITBL.table_catalog
    AND DWTBL.table_name=NONPHITBL.table_name
    """

current_session.sql(sql_statement).collect()
dataframe = current_session.table("SUMMARY_COLUMN_CNT_TABLE_VIEWS")
streamlit.dataframe(dataframe.show())

# streamlit.multiselect("Pick some fruits:", list(my_fruit_list.index))


