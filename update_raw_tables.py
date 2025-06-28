import os
import requests
import json
import html
import pandas as pd
import numpy as np

from datetime import datetime
import psycopg2
from psycopg2 import sql, Error
from psycopg2.extras import execute_values


###########################################################################
### API parameters
###########################################################################

# API headers
headers = {
    "User":   os.getenv("VC_USER",   "user-uuid"),  
    "Key":    os.getenv("VC_KEY",    "key"),        
    "Secret": os.getenv("VC_SECRET", "secret")
}

# Function to connect to VisualCare public API and return dataframe
def get_visualcare_data(url, headers=headers, timeout=30):
    """
    Sends a request to VisualCare public API. Returns pandas datfarame.
    
    Parameters:
    - url: API request url
    - headers: user, key and secret for API authentication
    - timeout: the default timeout is 30 seconds
    """
    print(f"✅ [{datetime.now()}] Send request to VisualCare public API.")
    
    # Make the request
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)  
        resp.raise_for_status()
    except requests.exceptions.RequestException as err:
        raise SystemExit(f"Request failed: {err}")            
    
    # Get json file
    json = resp.json()
    
    # Convert json file to pandas dataframe
    df = pd.DataFrame(json)

    # Lowercase column names
    df.columns = [col.lower() for col in df.columns]  

     # Add timestamp column
    df['modifiedtime'] = datetime.now()

    print(f"✅ [{datetime.now()}] Success: dataframe created with {len(df)} rows.")
    
    return df



# Function to connect to VisualCare public API and get json
def get_visualcare_data_json(url, headers=headers, timeout=30):
    """
    Sends a request to VisualCare public API. Returns json.
    
    Parameters:
    - url: API request url
    - headers: user, key and secret for API authentication
    - timeout: the default timeout is 30 seconds
    """
    print(f"✅ [{datetime.now()}] Send request to VisualCare public API.")
    
    # Make the request
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)  
        resp.raise_for_status()
    except requests.exceptions.RequestException as err:
        raise SystemExit(f"Request failed: {err}")            
    
    # Get json file
    json = resp.json()
    
    print(f"✅ [{datetime.now()}] Success: json file created with {len(json)} rows.")
    
    return json



###########################################################################
### Open PostgreSQL connection
###########################################################################

# PostgreSQL connection parameters
conn = psycopg2.connect(
    user = 'database_owner',
    password = os.getenv("RENDER_USER_PASSWORD", "render-user-password"),
    host='dpg-d1fptvvfte5s73fqj340-a.singapore-postgres.render.com',
    port='5432',
    database='sensible_care'
)

# Deactivate auto commit
conn.autocommit = False

# Deactivate auto commit
conn.autocommit = False

# Open a cursor to perform database operations
cursor = conn.cursor()

# Tell the session to search for SensibleCare schema
cursor.execute(
    sql.SQL("SET search_path TO {s};")
    .format(s=sql.Identifier("SensibleCare"))
)


def create_table_from_df(df, schema_name, table_name, conn, primary_key):
    """
    Create a PostgreSQL table from a pandas DataFrame using psycopg2.
    Returns a message indicating whether the table was created or already exists.

    Parameters:
    - df: pandas DataFrame
    - schema_name: str, target schema
    - table_name: str, target table name
    - conn: psycopg2 connection object
    - primary_key: str or list of str, optional column(s) to use as primary key
    """

    # Step 1: Check if table exists
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s 
                  AND table_name = %s
            )
        """, (schema_name, table_name))
        table_exists = cursor.fetchone()[0]
    
    # Step 2: Create table if not exists
    if not table_exists:
        dtype_mapping = {
            'int64': 'INTEGER',
            'float64': 'DOUBLE PRECISION',
            'object': 'TEXT',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP'
        }
    
        columns = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            pg_type = dtype_mapping.get(dtype, 'TEXT')
            col_def = sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(pg_type))
            columns.append(col_def)
    
        create_query = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            sql.SQL(', ').join(columns)
        )
        
        with conn.cursor() as cursor:
            cursor.execute(create_query)
            conn.commit()
            print(f"✅ [{datetime.now()}] Success: table '{schema_name}.{table_name}' created successfully.")

        # Step 3: Add PRIMARY KEY using ALTER TABLE
        pk_list = [primary_key] if isinstance(primary_key, str) else primary_key

        alter_query = sql.SQL("ALTER TABLE {}.{} ADD PRIMARY KEY ({})").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, pk_list))
        )

        try:
            with conn.cursor() as cursor:
                cursor.execute(alter_query)
                conn.commit()
                print(f"🔑 [{datetime.now()}] Primary key added on column(s): {', '.join(pk_list)}")
        except Exception as e:
            conn.rollback()
            print(f"⚠️ Failed to add primary key: {e}")
        
    else:
        print(f"⚠️ [{datetime.now()}] Info: Table '{schema_name}.{table_name}' already exists.")    



def upsert_table(df, schema_name, table_name, conn, primary_key):
    """
    Perform UPSERT (insert or update) from a pandas DataFrame to an existing PostgreSQL table.

    Parameters:
    - df: pandas DataFrame
    - schema_name: str, target schema
    - table_name: str, target table name
    - conn: psycopg2 connection object
    - primary_key: str or list of str, column(s) used for conflict resolution
    """
    
    # Check if dataframe is not empty
    if df.empty:
        print(f"Warning: DataFrame for table '{table_name}' is empty. Skipping upsert.")
        return

    # Create tuples for data insertion
    tuples = [tuple(row) for row in df.to_numpy()]
    columns = list(df.columns)

    # Ensure primary_key is a list
    if isinstance(primary_key, str):
        pk_list = [primary_key]
    else:
        pk_list = primary_key

    # Build upsert SQL statement
    upsert_query = sql.SQL("""
        INSERT INTO {}.{} ({})
        VALUES %s
        ON CONFLICT ({}) DO UPDATE SET {}
    """).format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        sql.SQL(', ').join(map(sql.Identifier, pk_list)),
        sql.SQL(', ').join([
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in columns if c not in pk_list
        ])
    )

    try:
        with conn.cursor() as cursor:
            execute_values(cursor, upsert_query.as_string(conn), tuples)
            conn.commit()
            print(f"✅ [{datetime.now()}] Success: table '{schema_name}.{table_name}' updated with {len(df)} rows.")

    except Error as e:
        conn.rollback()
        print("❌ PostgreSQL Error:", e.pgcode, e.pgerror)
        raise


###########################################################################
### Upsert table 'clients'
###########################################################################

# Get data from the API
url = 'https://publicapi.visualcare.com.au/clients?includeNonCurrent=TRUE&includeNotes=FALSE'
clients = get_visualcare_data(url)

target_columns = [
    'clientid',
     'clientcode',
     'firstname',
     'lastname',
     'preferredname',
     'gender',
     'maritalstatus',
     'dateofbirth',
     'fundingtype',
     'clienttype',
     'hcplevel',
     'payerid',
     'suburb',
     'state',
     'postcode',
     'longitude',
     'latitude',
     'area',
     'division',
     'casemanager',
     'casemanager2',
     'languageenglish',
     'languageother',
     'interpreterrequired',
     'referral',
     'referer',
     'referercode',
     'currentstatus',
     'servicestatus',
     'servicestart',
     'serviceend',
     'current',
     'reasonserviceended',
     'preferedworker',
     'nonpreferedworker',
     'modifiedtime'
]

# Subset dataframe
clients_df = clients[target_columns].copy()
clients_df = clients_df.applymap(lambda x: html.unescape(x) if isinstance(x, str) else x)

create_table_from_df(clients_df, 'raw', 'clients', conn, 'clientid')
upsert_table(clients_df, 'raw', 'clients', conn, 'clientid')


###########################################################################
### Cleanup
###########################################################################

# Close communication with the database
cursor.close()
conn.close()



