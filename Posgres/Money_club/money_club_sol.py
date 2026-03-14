import json
import logging
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import extras

logging.basicConfig(level=logging.INFO)

# Database connection parameters
dbname = "postgres"
user = ""
password = ""
host = "localhost"
port = "5432"


# Establish a connection to PostgreSQL
def connect_db():
    try:
        connection = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )
        logging.info("Connected to the database!")
        return connection
    except Exception as e:
        logging.error(f"Error: {e}")


def close_connection(connection):
    # Close the connection
    if connection:
        connection.close()
        logging.info("Connection closed.")


def create_table(connection):
    # Create a cursor object to interact with the database
    with connection.cursor() as cursor:
        # Define SQL statements for table creation
        create_customer_table = """
        CREATE TABLE IF NOT EXISTS customer (
            customer_id SERIAL PRIMARY KEY,
            first_name VARCHAR(20) NOT NULL,
            last_name VARCHAR(20) NOT NULL,
            date_of_birth TIMESTAMP
        )
        """

        create_transactions_table = """
        CREATE TABLE IF NOT EXISTS transactions (
            txn_id SERIAL PRIMARY KEY,
            customer_id INTEGER REFERENCES customer(customer_id),
            txn_type VARCHAR(20),
            txn_amount DECIMAL(10, 2) NOT NULL,
            transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        # Execute SQL statements
        cursor.execute(create_customer_table)
        cursor.execute(create_transactions_table)

        # Commit the changes
        connection.commit()
        logging.info("Tables 'customer' and 'transactions' created successfully!")


def insert_data(connection):
    customer_data = [
        ("John", "Doe", "2000-01-15"),
        ("Alice", "Smith", "2002-04-22"),
        ("Bob", "Johnson", "2002-09-05"),
        ("Eva", "Williams", "2003-12-10"),
        ("Charlie", "Brown", "2005-03-28"),
    ]

    transactions_data = [
        (1, "Credit", 500.50, "2023-01-15"),
        (2, "Debit", 200.75, "2023-01-15"),
        (3, "Credit", 100.25, "2023-01-15"),
        (1, "Debit", 300.00, "2023-01-15"),
        (2, "Credit", 150.50, "2023-01-15"),
    ]

    # Create a cursor object to interact with the database
    with connection.cursor() as cursor:
        # Insert data into the 'customer' table
        insert_customer_query = """
        INSERT INTO customer (first_name, last_name, date_of_birth)
        VALUES %s
        """
        extras.execute_values(cursor, insert_customer_query, customer_data)

        # Insert data into the 'transactions' table
        insert_transactions_query = """
        INSERT INTO transactions (customer_id, txn_type, txn_amount, transaction_date)
        VALUES %s
        """
        extras.execute_values(cursor, insert_transactions_query, transactions_data)

        # Commit the changes
        connection.commit()
        logging.info("Data inserted successfully!")


def calculate_age(date_of_birth, reference_date):
    # birth_date = datetime.strptime(date_of_birth, '%Y-%m-%d')
    age = (reference_date - date_of_birth).days // 365
    return age


def calculate_savings(events, context):
    try:
        # Step 1: Read the date from the incoming events payload
        payload = json.loads(events)
          # Step 2: Connect to the PostgreSQL database
        connection = psycopg2.connect(
            dbname=payload["database"],
            user=payload["username"],
            password=payload["password"],
            host=payload["host"],
            port=payload["port"],
        )
        cursor = connection.cursor()

        target_date = datetime.strptime(payload["target_date"], "%Y-%m-%d")

        # Step 3: Read all transactions from the provided date and calculate savings
        savings_data = {}
        cursor.execute(
            "SELECT customer_id, txn_type, txn_amount, transaction_date FROM transactions WHERE transaction_date = %s",
            (target_date,),
        )
        for row in cursor.fetchall():
            customer_id, txn_type, txn_amount, txn_date = row
            txn_amount = txn_amount * (1 if txn_type == "Credit" else -1)

            if customer_id not in savings_data:
                savings_data[customer_id] = {"total_savings": 0, "count": 0}
            savings_data[customer_id]["total_savings"] += txn_amount
            savings_data[customer_id]["count"] += 1
        # Step 4-7: Calculate average savings per age group
        age_savings = {}
        cursor.execute("SELECT customer_id, date_of_birth FROM customer")
        for row in cursor.fetchall():
            customer_id, date_of_birth = row

            if customer_id in savings_data:
                age = calculate_age(date_of_birth, target_date)
                total_savings = savings_data[customer_id]["total_savings"]
                count = savings_data[customer_id]["count"]

                if age not in age_savings:
                    age_savings[age] = {"total_savings": 0, "count": 0}
                age_savings[age]["total_savings"] += total_savings
                age_savings[age]["count"] += count
        # Calculate average savings and prepare the response payload
        response_data = {}
        for age, data in age_savings.items():
            avg_saving = (
                data["total_savings"] / data["count"] if data["count"] != 0 else 0
            )
            response_data[int(age)] = round(avg_saving)
        response_payload = {"statusCode": 200, "data": response_data}
    except Exception as e:
        response_payload = {"statusCode": 400, "message": str(e)}
    finally:
        # Close the database connection
        if connection:
            connection.close()
    return json.dumps(response_payload)


def main():
    # money_club_db = connect_db()
    # create_table(money_club_db)
    # insert_data(money_club_db)
    # close_connection(money_club_db)

    events = """
    {
        "database": "postgres",
        "username": "postgres",
        "password": "",
        "host": "",
        "port": "5432",
        "target_date": "2023-01-15"
    }
    """
    result = calculate_savings(events, None)
    logging.info(result)


if __name__ == "__main__":
    logging.debug("Start")
    main()
    logging.debug("End")
