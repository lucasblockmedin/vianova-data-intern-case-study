#!/usr/bin/env python

"""
Downloads a dataset of cities with a population of 1000 or more, stores it in a SQLite database,
and retrieves countries without a megacity. The results are saved to a tab-separated value (TSV) file.

This script is intended to be run automatically on a weekly basis to update the resulting data.

"""

import requests
import sqlite3
import csv
import io
import constants
import logging


__author__ = "Lucas Block"
__copyright__ = "Vianova"
__credits__ = ["Lucas"]
__license__ = ""
__version__ = "1"
__maintainer__ = "Lucas Block"
__email__ = ""
__status__ = "Production"


def download_dataset(url, headers):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        logging.warning("Failed to download the dataset.")
        raise Exception("Failed to download the dataset. Status code:", 
                        response.status_code)


def create_table(cursor, table_name, header):
    column_names = []
    column_types = []
    for column in header:
        column_names.append(column)
        if column == "population":
            column_types.append("INTEGER")
        else:
            column_types.append("TEXT")
    create_table_query = f"""CREATE TABLE IF NOT EXISTS {table_name} 
                            ({', '.join([f'{name} {typ}' 
                            for name, typ 
                            in zip(column_names, column_types)])})"""
    cursor.execute(create_table_query)
    logging.info("Table created successfully.")


def insert_rows(cursor, table_name, reader):
    for row in reader:
        insert_query = f"""INSERT INTO {table_name} 
                        VALUES ({','.join(['?'] * len(row))});"""
        cursor.execute(insert_query, row)


def retrieve_countries_without_megacity(cursor, table_name):
    query = f"""
    SELECT country_code, cou_name_en
    FROM {table_name}
    WHERE country_code NOT IN (
        SELECT country_code
        FROM {table_name}
        WHERE population >= 10000000
        GROUP BY country_code
    )
    GROUP BY country_code
    ORDER BY cou_name_en;
    """
    cursor.execute(query)
    return cursor.fetchall()


def save_results_to_tsv(results, output_file):
    with open(output_file, "w", newline="") as file:
        writer = csv.writer(file, delimiter="\t")
        writer.writerow(["country_code", "country_name"])
        writer.writerows(results)


def main():
    # Set the API endpoint URL
    url = constants.URL

    # Set your API key if required
    api_key = False

    # Set the SQLite database file path
    database_path = constants.DB_PATH

    # Set the output file path
    output_file = constants.RES_PATH

    # Set the table name for the database
    table_name = constants.TABLENAMES

    # Set the request headers
    headers = {"Authorization": api_key} if api_key else {}

    # Download the dataset
    csv_data = download_dataset(url, headers)

    # Connect to the SQLite database
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # Read the CSV data using io.StringIO as a file-like object
    csv_file = io.StringIO(csv_data)

    # Read the CSV data using csv.reader
    reader = csv.reader(csv_file, delimiter=';')

    # Read the header row to get the column names
    header = next(reader, None)

    # Create a table in the database with the determined columns
    create_table(cursor, table_name, header)

    # Insert each row into the database table
    insert_rows(cursor, table_name, reader)

    # Commit the changes and close the database connection
    conn.commit()

    logging.info("Database created and populated successfully.")

    # Retrieve countries without a megacity
    results = retrieve_countries_without_megacity(cursor, table_name)

    # Save the results to a tab-separated value (TSV) file
    save_results_to_tsv(results, output_file)

    # Close the database connection
    conn.close()

    logging.info("Results saved to the output file successfully.")


if __name__ == "__main__":
    logging.basicConfig(level=constants.LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
