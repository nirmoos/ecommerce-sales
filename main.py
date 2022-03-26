import argparse
import csv
import json
import re

import mysql.connector
import os
import sys

from dotenv import load_dotenv
from mysql.connector import errorcode
from tabulate import tabulate

from queries import *
from utils import parse_datetime


load_dotenv(".env")


CSV_TO_DB_KEY_TYPE_MAPPING = (
    {
        "InvoiceNo": ("invoice_no", lambda x: "'%s'" % x),
        "StockCode": ("stock_code", lambda x: "'%s'" % x),
        "Description": ("description", lambda x: json.dumps(x)),
        "Quantity": ("quantity", lambda x: x),
        "InvoiceDate": ("date", lambda x: "'%s'" % parse_datetime(x).strftime("%Y-%m-%d %H:%M:%S")),
        "UnitPrice": ("unit_price", lambda x: x),
        "CustomerID": ("customer_id", lambda x: x),
        "Country": ("country", lambda x: json.dumps(x))
    }
)


class EcommerceSale:
    """Ecommerce sale class allows to interact with sales data"""
    def __init__(self):
        """Initializes the sql connection and cursor objects for database operations"""
        try:
            self.cnx = self.create_db_connection()
            self.cursor = self.cnx.cursor()
        except KeyError:
            print("Please set the environment variables prior to run")
            sys.exit(1)
        except mysql.connector.Error as err:
            if err.errno == errorcode.CR_CONN_HOST_ERROR:
                print("Please make sure provided HOST is actively serving")
            elif err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Please check your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            elif err.errno == errorcode.ER_DBACCESS_DENIED_ERROR:
                print("User doesn't have permission to the given database")
            else:
                print(err)
            sys.exit(1)

    def __del__(self):
        """Closes the sql connection and cursor objects while closing the application"""
        try:
            self.cursor.close()
            self.cnx.close()
        except AttributeError:
            pass

    def load_data(self, force_load):
        """Handles the decision logic for the load operation"""
        self.cursor.execute(TABLE_CHECK_AND_ROW_COUNT_QUERY)
        result = list(self.cursor)

        # Creates the table and load data if the table is not present
        if not result:
            self.cursor.execute(CREATE_TABLE_QUERY)
            self._load_csv_data_to_table()
        # Truncate the table and load data, especially if the  force_load command-line option is provided as true
        # Useful if we want to reload the table with the CSV after we have modified the table
        elif not result[0][1] or force_load:
            self.cursor.execute(TRUNCATE_TABLE_QUERY)
            self._load_csv_data_to_table()
        else:
            pass

    def _load_csv_data_to_table(self):
        """Handles the logic for loadig data from CSV to database table, inserts 10000 records on every write op"""
        insert_query_keys = list(map(lambda x: x[1][0], CSV_TO_DB_KEY_TYPE_MAPPING.items()))
        for batch_of_rows in self._fetch_csv_data_as_batches_of_10000():
            formatted_insert_query = DATA_LOADING_QUERY % (
                ",".join(insert_query_keys),
                ",".join([
                    "(%s)" % ",".join([
                        y(row[k]) if row[k] else "NULL" for k, (x, y) in CSV_TO_DB_KEY_TYPE_MAPPING.items()
                    ]) for row in batch_of_rows
                ])
            )
            self.cursor.execute(formatted_insert_query)
            self.cnx.commit()

    def fetch_most_purchased_customers(self):
        """Fetches most purchased customers"""
        self.cursor.execute(CUSTOMER_PURCHASED_HIGHEST_QUANTITY_QUERY)
        result = list(self.cursor)

        return result

    def fetch_top10_customers_who_spent_most(self):
        """Fetches top10 most spent customers"""
        self.cursor.execute(TOP_10_CUSTOMERS_WITH_SALES_AMOUNT)
        result = list(self.cursor)

        return result

    def fetch_most_purchased_products(self):
        """Fetches top10 most purchased products"""
        self.cursor.execute(TOP_10_PRODUCTS_PURCHASES)
        result = list(self.cursor)

        return result

    @staticmethod
    def _fetch_csv_data_as_batches_of_10000():
        """Acts like an iterator, and provides 10000 records on every iteration"""
        file = open("./data/data.csv", encoding="ISO-8859-1")
        reader = csv.DictReader(file, delimiter=",")

        batch_of_rows = []
        for index, row in enumerate(reader):
            batch_of_rows.append(row)
            if index % 10000 == 0:
                yield batch_of_rows
                batch_of_rows = []
        if batch_of_rows:
            yield batch_of_rows

        file.close()

    @staticmethod
    def create_db_connection():
        """Creates a database connection"""
        return mysql.connector.connect(
            host=os.environ["DB_HOST_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            database=os.environ["DB_DATABASE"]
        )


def main(force_load):
    """main function only handles the high level logic of the program"""
    ecommerce_sale = EcommerceSale()
    ecommerce_sale.load_data(force_load)

    print("\n\033[95mMost purchased customers over the days:\033[96m\n")
    result = ecommerce_sale.fetch_most_purchased_customers()
    print(tabulate(result, headers=["Date", "Customer ID", "Quantity"]))

    print("\n\033[95mTop 10 customers who spent the most:\033[96m\n")
    result = ecommerce_sale.fetch_top10_customers_who_spent_most()
    print(tabulate(result, headers=["Customer ID", "Sales Amount"]))

    print("\n\033[95mTop 10 products purchased more:\033[96m\n")
    result = ecommerce_sale.fetch_most_purchased_products()
    print(tabulate(result, headers=["Stock Code", "Stock Name", "Total Sale"]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force-load", help="Provide 'true'/'false' for truncating and re-render data to table",
        nargs="?",
        type=str,
        default="false"
    )
    args = parser.parse_args()

    # If force load is true, then the table gets truncated and reloaded
    main(force_load=bool(re.match("^true|0$", args.force_load.lower())))
