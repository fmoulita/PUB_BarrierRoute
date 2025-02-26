# -*- coding: utf-8 -*-
"""
Created on Mon Feb  5 19:47:27 2024

@author: della
"""

import os
import pyodbc

databaseFld = ""
newRouteFld = ""

print("Loading Settings")
current_dir = os.path.dirname(os.path.abspath(__file__))
settings_file = os.path.join(current_dir, "data", "settings.txt")

def read_settings_file(settings_file):
    settings = {}
    if os.path.exists(settings_file):
        with open(settings_file, 'r') as f:
            for line in f:
                line = line.split("#")[0].strip()
                if line:
                    var_name, var_type, var_value = line.split("\t")
                    if var_type == 'list':
                        settings[var_name] = var_value.split(',')
                    elif var_type == 'str':
                        settings[var_name] = var_value
                    else:
                        settings[var_name] = var_type(var_value)
    else:
        print(f"Settings file '{settings_file}' not found.")
    return settings

def insert_data_from_files(mdb_files, input_files, table_name):
    for mdb_file, input_file in zip(mdb_files, input_files):
        conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={mdb_file};"
        print("mdb_file : ", mdb_file)
        print("input_file : ", input_file)
        insert_data_into_database(conn_str, input_file, table_name)

def insert_data_into_database(conn_str, input_file, table_name):
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {table_name}")

        # Read the data from the input file
        with open(input_file, 'r') as f:
            lines = f.readlines()

        # Insert each row into the specified table
        for line in lines:
            dataline = line.strip().split('\t')
            sql = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in range(len(dataline))])})"
            cursor.execute(sql, dataline)

        conn.commit()
        conn.close()
        print(f"Data inserted successfully into table '{table_name}'.")

    except Exception as e:
        print(f"Error inserting data into table '{table_name}': {e}")

# Specify the settings file
settings_file = os.path.join(current_dir, "data", "settings.txt")
settings = read_settings_file(settings_file)

# Extract database MDB files, table name and input text files from settings
mdb_files = [os.path.join(settings["databaseFld"], mdb) for mdb in settings.get("mdb_files", [])]
input_files = [os.path.join(settings["newRouteFld"], input_file) for input_file in settings.get("input_files", [])]
table_name = "VslArrivalGen"

# Insert data into the database for each pair of MDB and input files
insert_data_from_files(mdb_files, input_files, table_name)
