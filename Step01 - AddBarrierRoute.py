# -*- coding: utf-8 -*-
"""
Created on Thu Feb  1 07:54:43 2024
@author: della
"""

import os
import pyodbc
import time

databaseFld = ""
configFld = ""

print("Loading Settings")
current_dir = os.path.dirname(os.path.abspath(__file__))
settings_file = os.path.join(current_dir, "data", "settings.txt")
print("settings_file :", settings_file)

if os.path.exists(settings_file):
    with open(settings_file, 'r') as f:
        for line in f:
            line = line.split("#")[0].strip()
            if line:
                dataline = line.split("\t")
                if len(dataline) == 3:
                    var_name, var_type, var_value = dataline
                    if var_type == 'list':
                        exec(f"{var_name} = {var_value.split(',')}")
                    elif var_type == 'str':
                        exec(f"{var_name} = '{var_value}'")
                    else:
                        exec(f"{var_name} = {var_type}({var_value})")
                    print(f"Set {var_name} as {var_value}")
else:
    print(f"Settings file '{settings_file}' not found.")

def service_point_barrier_map():
    """
    Read service points and their corresponding codes from the BarrierServicePoint.txt file.
    """
    service_points = {}
    barrier_service_point_file = os.path.join(configFld, "BarrierServicePoint.txt")
    
    try:
        with open(barrier_service_point_file, 'r') as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) > 2:
                    service_point, _, code = parts
                    service_points[service_point] = code
    except FileNotFoundError as ex:
        print(f"Error: {ex}")
    return service_points


def modify_route_code(route_code, service_point_codes):
    """
    Modify the route code based on the service point codes.
    """

    area_map = {service_point: code[:2] for service_point, code in service_point_codes.items()}
    parts = route_code.split(":")
    modified_parts = []

    for i, part in enumerate(parts):
        if part in service_point_codes:
            if i > 0 and parts[i-1] not in service_point_codes:  # If it's not the first part and the previous part is not a service point
                modified_parts.append(f"InBarrier{service_point_codes[part]}")
            modified_parts.append(f"{part}")
            if i < len(parts) - 1 and parts[i + 1] in service_point_codes:
                next_area = area_map.get(parts[i + 1])
                curr_area = area_map.get(part)
                if next_area != curr_area:
                    modified_parts.append(f"OutBarrier{curr_area}")
                    modified_parts.append(f"InBarrier{next_area}")
            if i < len(parts) - 1 and parts[i+1] not in service_point_codes:
                modified_parts.append(f"OutBarrier{service_point_codes[part]}")
        else:
            modified_parts.append(part)

    # Check if the last InBarrier tag has a corresponding OutBarrier tag
    last_index = len(modified_parts) - 1
    if modified_parts[last_index].startswith("InBarrier"):
        last_area = modified_parts[last_index][10:]  # Extract area from "InBarrier" tag
        last_service_point = parts[-1]
        if last_service_point in service_point_codes:
            last_service_point_area = area_map.get(last_service_point)
            if last_service_point_area != last_area:
                modified_parts.append(f"OutBarrier{last_area}")

    modified_route_code = ":".join(modified_parts)
    return modified_route_code


def write_updated_route_codes_to_file(rows, output_file):
    """
    Write the updated route codes to a text file.
    """
    if os.path.exists(output_file):
        os.remove(output_file)

    with open(output_file, 'a') as f:
        for row in rows:
            f.write(f"{row}\n")

def update_route_codes_in_database(conn_str, service_point_codes, mdb_file, output_file, max_retries=3, retry_delay=1):
    updated_rows = []
    
    retry_count = 0
    max_retries = int(max_retries)
    while retry_count < max_retries:
        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM VslArrivalGen")
            rows = cursor.fetchall()

            for row in rows:
                old_route_code = row.RouteCode
                new_route_code = modify_route_code(old_route_code, service_point_codes)
                # Update the route code in the row
                row.RouteCode = new_route_code
                draft = row[6]
                if int(draft) == draft:
                    draft_formatted_value = str(int(draft))
                else:
                    rounded_value = round(draft, 1)  # Round to one decimal place
                    draft_formatted_value = "{:,.1f}".format(rounded_value).replace(".", ",")
                # Append the updated row to the list
                row.Draft = draft_formatted_value
                updated_rows.append(row)
            
            print("Route codes updated successfully.")
            
            conn.close()
            write_updated_route_codes_to_file(updated_rows, output_file)  # Write updated rows to file
            return

        except Exception as e:
            print(f"Error updating route codes in database: {e}")
            print("Retrying...")
            retry_count += 1
            time.sleep(retry_delay)

    print("Max retries reached. Could not update route codes in database.")

def write_updated_route_codes_to_file(updated_rows, output_file):
    """
    Write the updated rows to a text file.
    """
    with open(output_file, 'a') as f:
        for row in updated_rows:
            row_str = '\t'.join(map(str, row))
            f.write(f"{row_str}\n")

# List all files in the database directory
database_files = os.listdir(databaseFld)
mdb_files = [f for f in database_files if f.endswith('.mdb')]
output_dir = os.path.join(current_dir, "data")

# Update route codes in each MDB file and append to the output file
for mdb_file in mdb_files:
    service_point_codes = service_point_barrier_map()
    mdb_path = os.path.join(databaseFld, mdb_file)
    output_file = os.path.join(output_dir, os.path.splitext(os.path.basename(mdb_file))[0] + "_updated_route_codes_barrier.txt")
    print(f"Updating route codes in '{mdb_file}':")
    conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={mdb_path};"
    update_route_codes_in_database(conn_str, service_point_codes, mdb_file, output_file)
