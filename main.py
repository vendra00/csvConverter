import csv
import os

import snowflake.connector
import logging
import colorlog
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv('.env')
DELIMITER = '|'  # define the delimiter here
count: int = 0  # initialize count variable here
line_number: int = 0  # initialize line_number variable here

# Set up logging with color output
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s%(asctime)s - %(levelname)s - %(message)s'))
logger = colorlog.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Set the logging level for the Snowflake Connector logger to WARNING
logging.getLogger("snowflake.connector").setLevel(logging.WARNING)


# Define a function to print a connection message
def print_connection_message(user, account):
    print(f"Connected to Snowflake as {user} on account {account}")


# Set up a connection to Snowflake
logging.info("Connecting to Snowflake...")
cnx = snowflake.connector.connect(
    user=os.environ.get('SNOWFLAKE_USER'),
    password=os.environ.get('SNOWFLAKE_PASSWORD'),
    account=os.environ.get('SNOWFLAKE_ACCOUNT'),
    region=os.environ.get('SNOWFLAKE_REGION'),
    warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
    database=os.environ.get('SNOWFLAKE_DATABASE'),
    schema=os.environ.get('SNOWFLAKE_SCHEMA'),
    role=os.environ.get('SNOWFLAKE_ROLE')
)

# Call the function to print the connection message
logging.info("Connecting to snowflake...")
print_connection_message(cnx.user, cnx.account)

# Create a cursor object
logging.info("Creating cursor...")
cursor = cnx.cursor()

cursor.execute("USE DATABASE dev_inatlasdb")

# Define the SQL INSERT statement
insert_stmt = ("INSERT INTO c_verdnatura.aux_table "
               "(cif_original, cif_actual, oid_empresa, oid_centro) "
               "VALUES (%s, %s, %s, %s)")

try:
    # Check if the file is empty
    if os.stat('data.txt').st_size == 0:
        raise Exception("Data file is empty")

    # Open the text file and read the data
    logging.info("Reading data from file...")
    with open('data.txt') as csvfile:
        reader = csv.reader(csvfile, delimiter=DELIMITER)
        next(reader)  # skip the header row
        for row in reader:
            line_number += 1
            try:
                # Insert each row into the Snowflake table
                logging.info(f"Inserting row: {row}")
                cursor.execute(insert_stmt, row)
                count += 1
            except Exception as e:
                logging.error(f"Error occurred on line {line_number}: {str(e)}")
    logging.info(f"Inserted {count} rows into Snowflake")
except Exception as e:
    logging.error(f"Error occurred: {str(e)}")

# Commit the changes and close the connection
logging.info("Committing changes...")
cnx.commit()
logging.info(f"Closing cursor and connection... Rows inserted: {count}")
cursor.close()
cnx.close()
