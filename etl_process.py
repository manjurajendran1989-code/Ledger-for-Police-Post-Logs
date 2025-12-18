import pandas as pd
import mysql.connector
import numpy as np

# 1. SETUP & READ DATA
# ---------------------------------------------------------
file_path = r"C:\Users\rajenm1\OneDrive - Ecolab\Documents\VS_Code_Python\traffic_stops - traffic_stops_with_vehicle_number.csv"
print("Reading CSV...")
df = pd.read_csv(file_path, low_memory=False)

# 2. DATA CLEANING
# ---------------------------------------------------------
# Clean Column Names
df.columns = [c.strip().lower() for c in df.columns]

# Handle Dates
if 'stop_date' in df.columns:
    df['stop_date'] = pd.to_datetime(df['stop_date'], errors='coerce').dt.date

# Handle Times
if 'stop_time' in df.columns:
    df['stop_time'] = pd.to_datetime(df['stop_time'], format='%H:%M:%S', errors='coerce').dt.time

# Handle Age
if 'driver_age' not in df.columns and 'driver_age_raw' in df.columns:
    df['driver_age'] = pd.to_numeric(df['driver_age_raw'], errors='coerce')
elif 'driver_age' in df.columns:
    df['driver_age'] = pd.to_numeric(df['driver_age'], errors='coerce')

# Handle Booleans
bool_cols = ['search_conducted', 'is_arrested', 'drugs_related_stop']
bool_mapper = {
    'True': True, 'False': False, 'true': True, 'false': False,
    'TRUE': True, 'FALSE': False, '1': True, '0': False,
    1: True, 0: False, 'y': True, 'n': False, True: True, False: False
}

for c in bool_cols:
    if c in df.columns:
        df[c] = df[c].map(bool_mapper)

# Handle Violations
if 'violation_raw' in df.columns and 'violation' not in df.columns:
    def map_violation(s):
        if pd.isna(s): return 'Unknown'
        s0 = str(s).lower()
        if 'speed' in s0: return 'Speeding'
        if 'dui' in s0 or 'drunk' in s0: return 'DUI'
        if 'seat' in s0: return 'Seatbelt'
        if 'equipment' in s0: return 'Equipment'
        return str(s).strip().title()
    df['violation'] = df['violation_raw'].apply(map_violation)

# Fill remaining text NaNs with 'Unknown' (but keep numeric/dates as None for SQL)
text_cols = ['country_name', 'driver_gender', 'driver_race', 'violation_raw', 
             'violation', 'search_type', 'stop_outcome', 'stop_duration', 'vehicle_number']

for c in text_cols:
    if c in df.columns:
        df[c] = df[c].fillna('Unknown')

# 3. SQL UPLOAD
# ---------------------------------------------------------
try:
    conn_mysql = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Krithik@1229"
    )
    cursor_mysql = conn_mysql.cursor()
    
    # Init Database
    cursor_mysql.execute("CREATE DATABASE IF NOT EXISTS police_checkpost;")
    cursor_mysql.execute("USE police_checkpost;")
    
    # Re-create Table (Drop if exists to avoid duplicates during testing)
    cursor_mysql.execute("DROP TABLE IF EXISTS checkpost_stops;")
    cursor_mysql.execute(""" 
    CREATE TABLE checkpost_stops (
        id INT AUTO_INCREMENT PRIMARY KEY,
        stop_date DATE,
        stop_time TIME,
        country_name VARCHAR(100),
        driver_gender VARCHAR(20),
        driver_age_raw INT,
        driver_age INT,
        driver_race VARCHAR(50),
        violation_raw VARCHAR(200),
        violation VARCHAR(100),
        search_conducted BOOLEAN,
        search_type VARCHAR(100),
        stop_outcome VARCHAR(50),
        is_arrested BOOLEAN,
        stop_duration VARCHAR(50),
        drugs_related_stop BOOLEAN,
        vehicle_number VARCHAR(30)
    );
    """)
    print("Table created.")

    # 4. PREPARE DATA FOR INSERT
    # ---------------------------------------------------------
    # CRITICAL: We must select columns in the EXACT order of the SQL Query below
    # We also replace Pandas NaN/NaT with Python None for SQL compatibility
    
    sql_columns = [
        'stop_date', 'stop_time', 'country_name', 'driver_gender', 
        'driver_age_raw', 'driver_age', 'driver_race', 'violation_raw', 
        'violation', 'search_conducted', 'search_type', 'stop_outcome', 
        'is_arrested', 'stop_duration', 'drugs_related_stop', 'vehicle_number'
    ]
    
    # Ensure all columns exist, fill missing ones with None
    for col in sql_columns:
        if col not in df.columns:
            df[col] = None

    # Replace NaNs with None for SQL
    df_upload = df[sql_columns].where(pd.notnull(df), None)
    
    data_list = df_upload.values.tolist()

    query = """
    INSERT INTO checkpost_stops (
        stop_date, stop_time, country_name, driver_gender, 
        driver_age_raw, driver_age, driver_race, violation_raw, 
        violation, search_conducted, search_type, stop_outcome, 
        is_arrested, stop_duration, drugs_related_stop, vehicle_number
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    print(f"Inserting {len(data_list)} rows...")
    cursor_mysql.executemany(query, data_list)
    conn_mysql.commit()
    print("Success! Data inserted.")

except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    if 'conn_mysql' in locals() and conn_mysql.is_connected():
        cursor_mysql.close()
        conn_mysql.close()