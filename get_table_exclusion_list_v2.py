import argparse
import subprocess
import os
import logging
from datetime import datetime
import io
import csv
import sys

# Set up logging
def setup_logging(report_dir):
    log_file = os.path.join(report_dir, 'execution_log_{}.log'.format(datetime.now().strftime("%Y%m%d_%H%M%S")))
    logging.basicConfig(filename=log_file,
                        level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Logging started.")


def get_list_table_outside_warehouse(report_dir):
    # Define the SQL query
    query = '''
    SELECT A.NAME AS db_name, B.TBL_NAME AS table_name, C.LOCATION AS table_location, A.DB_LOCATION_URI
    FROM {}.DBS A
    JOIN {}.TBLS B ON A.DB_ID = B.DB_ID
    JOIN {}.SDS C ON B.SD_ID = C.SD_ID
    WHERE C.LOCATION NOT LIKE CONCAT('%', A.DB_LOCATION_URI, '%')
    '''.format(args.connect_db, args.connect_db, args.connect_db)

    # Add the predicate if query_db is not empty
    if args.query_db:
        db_list = args.query_db.split(',')
        query += " AND A.NAME IN ({})".format(", ".join("'{}'".format(db) for db in db_list))
    else:
        query += ";"
    print(query)

    # Command to execute the query
    mysql_command = [
        'mysql',  # MySQL client command
        '-h', args.host,  # MySQL host
        '-u', args.user,  # MySQL username
        '-P', args.port, # MySQL port
        '-p' + args.password,  # MySQL password (no space after -p)
        '-D', args.connect_db,  # Specify the database to connect to
        '--batch',  # Use batch mode for cleaner output
        '--silent',  # Suppress extra output
        '-e', query,  # Execute the query
    ]
    try:
        logging.info("Executing get_list_table_outside_warehouse query.")
        logging.info("MySQL Query: {}".format(query))
        result = subprocess.check_output(mysql_command, stderr=subprocess.PIPE)
        output = result.decode('utf-8').strip()  # Decode bytes to string
        logging.info("Query executed successfully.")

        # Prepare CSV output with headers
        output_io = io.StringIO(output)  # Changed from StringIO.StringIO
        reader = csv.reader(output_io, delimiter='\t')  # MySQL uses tabs in batch mode by default

        # Prepare CSV data
        csv_output = []
        headers = ["db_name", "table_name", "table_location", "db_location"]
        csv_output.append(headers)

        for row in reader:
            csv_output.append(row)

        # Save output to a file in the respective reports directory
        output_file = os.path.join(report_dir, 'create_table_exclude_filter_list_output.csv')
        with open(output_file, 'w', newline='', encoding='utf-8') as f:  # Changed mode to 'w' with encoding
            writer = csv.writer(f)
            writer.writerows(csv_output)
        logging.info("Output saved to {}".format(output_file))

    except subprocess.CalledProcessError as e:
        logging.error("Error executing query:")
        logging.error(e.output.decode('utf-8').strip())  # Decode error output


def create_table_exclude_filter_list(reports_dir):
    # Define the SQL query to get only the concatenated table names
    query = '''
    SELECT
        CONCAT('"', GROUP_CONCAT(B.TBL_NAME SEPARATOR '|'), '"') AS table_names
    FROM {}.DBS A
    JOIN {}.TBLS B ON A.DB_ID = B.DB_ID
    JOIN {}.SDS C ON B.SD_ID = C.SD_ID
    WHERE C.LOCATION NOT LIKE CONCAT('%', A.DB_LOCATION_URI, '%')
    '''.format(args.connect_db, args.connect_db, args.connect_db)

    # Add the predicate if query_db is not empty
    if args.query_db:
        db_list = args.query_db.split(',')
        query += " AND A.NAME IN ({})".format(", ".join("'{}'".format(db) for db in db_list))
    else:
        query += ";"

    # Command to execute the query
    mysql_command = [
        'mysql',  # MySQL client command
        '-h', args.host,  # MySQL host
        '-u', args.user,  # MySQL username
        '-P', args.port, # MySQL port
        '-p' + args.password,  # MySQL password (no space after -p)
        '-D', args.connect_db,  # Specify the database to connect to
        '--batch',  # Use batch mode for cleaner output
        '--silent',  # Suppress extra output
        '-e', query,  # Execute the query
    ]

    # Run the command
    try:
        logging.info("Executing create_table_exclude_filter_list query.")
        logging.info("MySQL Query: {}".format(query))
        result = subprocess.check_output(mysql_command, stderr=subprocess.PIPE)
        output = result.decode('utf-8').strip()  # Decode bytes to string
        file_path = "input_file.conf"
        
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        # Filter out lines starting with TABLES_EXCLUDE_LIST=
        filtered_lines = [line for line in lines if not line.startswith("TABLES_EXCLUDE_LIST=")]

        # Write the filtered lines back to the file
        with open(file_path, 'w', encoding='utf-8') as file:
            file.writelines(filtered_lines)

        line_to_insert = 'TABLES_EXCLUDE_LIST={}\n'.format(output)
        # Read the current content of the file
        lines = []
        # Check if the line exists and replace it
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                if line.startswith("TABLES_EXCLUDE_LIST="):
                    # Replace the line with the new one
                    lines.append(line_to_insert)
                else:
                    lines.append(line)

        # If the line didn't exist, add it
        if not any(line.startswith("TABLES_EXCLUDE_LIST=") for line in lines):
            lines.append(line_to_insert)

        # Write the updated content back to the file
        with open(file_path, 'w', encoding='utf-8') as file:
            file.writelines(lines)
        
        if file_path:
            print("Table exclude filter added to", file_path)      
        else:
            print("Script failed!!")
            sys.exit(1)  # Fixed typo: sys.ext(1) -> sys.exit(1)

        logging.info("Query executed successfully.")
        print("Query executed successfully.")

    except subprocess.CalledProcessError as e:
        print("Query execution failed!!")
        logging.error("Error executing query:")
        error_output = e.output.decode('utf-8').strip() if e.output else str(e)
        logging.error(error_output)  # Log any error message


def read_args_from_file(filename):
    args_dict = {}
    REQUIRED_KEYS = ["host", "user", "port", "password", "connect_db", "query_db", 
                     "create_table_exclude_list", "DB_NAME", "OZ_NAME", "EWD_NAME", 
                     "MAN_NAME", "HDFS_EWD_NAME"]

    with open(filename, 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith('#'):  # Skip empty lines and comments
                continue
            
            if '=' not in line:
                logging.error("Invalid line input_file: {}".format(line))
                logging.error("Script execution failed!!")
                sys.exit(1)  # Exit if any invalid line is encountered
            else:
                key, value = line.split('=', 1)  # Split only on the first '='
                args_dict[key] = value.strip('"')  # Remove quotes if present

    # Check for missing keys and print the first one found
    for key in REQUIRED_KEYS:
        if key not in args_dict:
            logging.error("Missing required key: {}".format(key))
            logging.error("Script execution failed!!")
            sys.exit(1)  # Exit if a required key is missing
    
    print("Input file validated!!", filename)
    return args_dict


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Query MySQL for table information.')
    parser.add_argument('-i', type=str, required=True, help='Input file with arguments.')
    args_from_file = read_args_from_file(parser.parse_args().i)

    # Create a namespace from the dictionary for easy access
    global args
    args = argparse.Namespace(**args_from_file)

    # Create a new directory for this run under reports
    report_run_dir = os.path.join('reports', 'run_get_table_exclusion_list_{}'.format(datetime.now().strftime("%Y%m%d_%H%M%S")))
    if os.path.exists(report_run_dir):
        print("Directory already exists:", report_run_dir)
        print("Script execution failed!!")
        sys.exit(1)  # Added exit to prevent continuing with existing directory
    else:
        os.makedirs(report_run_dir)
        # Verify if the directory was successfully created
        if not os.path.exists(report_run_dir):
            print("Failed to create directory:", report_run_dir)
            print("Script execution failed!!")
            sys.exit(1)
        else:
            print("Directory created successfully:", report_run_dir)

    # Set up logging
    setup_logging(report_run_dir)

    # Check the create_table_exclude_list flag
    if args.create_table_exclude_list == "true":
        logging.info("Creating table exclude list.")
        create_table_exclude_filter_list(report_run_dir)
    else:
        logging.info("Creating table exclude filter list.")
        get_list_table_outside_warehouse(report_run_dir)


if __name__ == "__main__":
    main()
