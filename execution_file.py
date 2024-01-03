import psycopg2
import time
import random
from faker import Faker
from datetime import date

fake = Faker()

# Database connection parameters
db_params = {
    'host': 'your_ec2_instance_ip',
    'port': 'your_postgresql_port',  # Default is usually 5432
    'database': 'your_database_name',
    'user': 'your_database_user',
    'password': 'your_database_password',
}

# Function to generate a hashed password
def generate_password_hash(password):
    return hashlib.sha256(password.encode()).hexdigest()

# Function to generate random date within a range
def generate_random_date(start_date, end_date):
    return str(date.fromordinal(random.randint(start_date.toordinal(), end_date.toordinal())))

# Function to generate random phone number
def generate_random_phone():
    return '+1' + ''.join([str(random.randint(0, 9)) for _ in range(9)])

# Function to generate random values for Customers table
def generate_random_customers(customer_id):
    return [
        customer_id,
        fake.first_name(),
        fake.last_name(),
        fake.email(),
        generate_random_phone(),
        fake.address(),
        fake.user_name(),
        generate_password_hash(fake.password())
    ]

# Function to generate random values for Subscriptions table
def generate_random_subscriptions(subscription_id, user_id):
    return [
        subscription_id,
        user_id,
        random.choice(["basic", "premium"]),
        generate_random_date("-1y", "now"),
        generate_random_date("now", "+1y")
    ]

# Function to generate random values for Products table
def generate_random_products(product_id):
    return [
        product_id,
        fake.word(),
        fake.text(),
        random.uniform(10, 1000),
        random.randint(1, 100)
    ]

# Function to generate random values for Reviews table
def generate_random_reviews(review_id, user_id, product_id):
    return [
        review_id,
        user_id,
        product_id,
        random.randint(1, 5),
        fake.sentence()
    ]

# Function to generate random values for Orders table
def generate_random_orders(order_id, user_id):
    return [
        order_id,
        user_id,
        generate_random_date("-1y", "now"),
        random.choice(["Shipped", "Pending", "Processing"])
    ]

# Function to generate random values for Order_Items table
def generate_random_order_items(order_item_id, order_id, product_id):
    return [
        order_item_id,
        order_id,
        product_id,
        random.randint(1, 10),
        random.uniform(10, 500)
    ]

# Function to generate random values for Payments table
def generate_random_payments(payment_id, user_id, order_id):
    return [
        payment_id,
        user_id,
        order_id,
        generate_random_date("now", "+1y"),
        random.uniform(10, 1000),
        random.choice(["Credit Card", "PayPal", "Bitcoin"])
    ]

# Function to insert random data into the PostgreSQL database
def insert_data_to_database(data, table_name, db_params):
    # Establish a connection to the PostgreSQL database
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    try:
        placeholders = ', '.join(['%s'] * len(data))
        columns = ', '.join([f'"{col}"' for col in table_columns[table_name]])
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(insert_query, data)
        
        # Commit the changes
        connection.commit()
        print(f"Data successfully inserted into the {table_name} table.")
    
    except Exception as e:
        print(f"Error: {e}")
        # Rollback the transaction in case of an error
        connection.rollback()
    
    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()

# Define the columns for each table
table_columns = {
    'Customers': ["customer_id", "first_name", "last_name", "email", "phone", "address", "username", "password_hash"],
    'Subscriptions': ["subscription_id", "user_id", "subscription_type", "start_date", "expiration_date"],
    'Products': ["product_id", "product_name", "description", "price", "stock_quantity"],
    'Reviews': ["review_id", "user_id", "product_id", "rating", "comment"],
    'Orders': ["order_id", "user_id", "order_date", "status"],
    'Order_Items': ["order_item_id", "order_id", "product_id", "quantity", "subtotal"],
    'Payments': ["payment_id", "user_id", "order_id", "payment_date", "amount", "payment_method"],
}


for _ in range(2):
    customer_id = fake.uuid4()
    user_id = customer_id
    subscription_id = fake.uuid4()
    product_id = fake.uuid4()
    review_id = fake.uuid4()
    order_id = fake.uuid4()
    order_item_id = fake.uuid4()
    payment_id = fake.uuid4()

    # Generate and insert one row of random data into each table
    customer_data = generate_random_customers(customer_id)
    insert_data_to_database(customer_data, 'Customers', db_params)

    subscription_data = generate_random_subscriptions(subscription_id, user_id)
    insert_data_to_database(subscription_data, 'Subscriptions', db_params)

    product_data = generate_random_products(product_id)
    insert_data_to_database(product_data, 'Products', db_params)

    review_data = generate_random_reviews(review_id, user_id, product_id)
    insert_data_to_database(review_data, 'Reviews', db_params)

    order_data = generate_random_orders(order_id, user_id)
    insert_data_to_database(order_data, 'Orders', db_params)

    order_item_data = generate_random_order_items(order_item_id, order_id, product_id)
    insert_data_to_database(order_item_data, 'Order_Items', db_params)

    payment_data = generate_random_payments(payment_id, user_id, order_id)
    insert_data_to_database(payment_data, 'Payments', db_params)

