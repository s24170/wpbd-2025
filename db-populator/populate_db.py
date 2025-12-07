import os
import random
import time
import signal
import sys
import argparse
from typing import List
from datetime import datetime

from faker import Faker
from sqlalchemy import create_engine, MetaData, Table, insert, update, select, Column, Integer, String, TIMESTAMP, \
    ForeignKey, Numeric
from dotenv import load_dotenv

fake = Faker()


def signal_handler(sig, frame):
    print("\nSimulation stopped gracefully")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def create_customer():
    return {
        "name": fake.name(),
        "email": fake.email(),
        "created_at": datetime.now()
    }


def create_product():
    return {
        "name": fake.word().capitalize() + " " + fake.word(),
        "price": round(random.uniform(5, 200), 2),
        "stock": random.randint(5, 100)
    }


def create_order(conn, customers_table, products_table):
    customer_result = conn.execute(select(customers_table.c.id)).fetchall()
    product_result = conn.execute(select(products_table.c.id, products_table.c.price)).fetchall()

    if not customer_result or not product_result:
        customer = create_customer()
        product = create_product()
        conn.execute(insert(customers_table), [customer])
        conn.execute(insert(products_table), [product])
        customer_id = conn.execute(select(customers_table.c.id).order_by(customers_table.c.id.desc())).scalar()
        product_id = conn.execute(select(products_table.c.id).order_by(products_table.c.id.desc())).scalar()
        price = float(conn.execute(select(products_table.c.price).where(products_table.c.id == product_id)).scalar())
    else:
        customer_id = random.choice(customer_result)[0]
        product_row = random.choice(product_result)
        product_id = product_row[0]
        price = float(product_row[1])

    quantity = random.randint(1, 5)

    return {
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": quantity,
        "total": round(price * quantity, 2),
        "ordered_at": datetime.now()
    }


def update_product_stock(conn, products_table):
    product_result = conn.execute(select(products_table.c.id)).fetchall()
    if not product_result:
        product = create_product()
        conn.execute(insert(products_table), [product])
        product_id = conn.execute(select(products_table.c.id).order_by(products_table.c.id.desc())).scalar()
    else:
        product_id = random.choice(product_result)[0]

    new_stock = random.randint(5, 100)

    return {
        "id": product_id,
        "stock": new_stock
    }


def build_db_url(db_provider: str):
    load_dotenv()
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_driver = 'psycopg2' if db_provider == 'postgresql' else 'null'

    url = f"{db_provider}+{db_driver}://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return url


def bulk_insert(engine, tables: List[Table], count: int = 1000):
    print(f"Starting bulk insert of {count} rows for each table...")
    with engine.begin() as conn:
        customers = [create_customer() for _ in range(count)]
        products = [create_product() for _ in range(count)]

        conn.execute(insert(tables[0]), customers)
        print(f"Inserted {count} customers")

        conn.execute(insert(tables[1]), products)
        print(f"Inserted {count} products")

        customer_ids = [row[0] for row in conn.execute(select(tables[0].c.id)).fetchall()]
        product_rows = [(row[0], float(row[1])) for row in
                        conn.execute(select(tables[1].c.id, tables[1].c.price)).fetchall()]

        orders = []
        for _ in range(count):
            customer_id = random.choice(customer_ids)
            product_id, price = random.choice(product_rows)
            quantity = random.randint(1, 5)

            orders.append({
                "customer_id": customer_id,
                "product_id": product_id,
                "quantity": quantity,
                "total": round(price * quantity, 2),
                "ordered_at": datetime.now()
            })

        conn.execute(insert(tables[2]), orders)
        print(f"Inserted {count} orders")

    print("Bulk insert completed!")


def run_continuous_simulation(engine, customers_table, products_table, orders_table):
    print("Starting continuous database write simulation...")
    print("Press Ctrl+C to stop the simulation")

    try:
        while True:
            operation = random.choices(['new_customer', 'new_product', 'new_order', 'update_stock'],
                                       weights=[20, 15, 50, 15], k=1)[0]

            with engine.begin() as conn:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                if operation == 'new_customer':
                    customer = create_customer()
                    conn.execute(insert(customers_table), [customer])
                    print(f"[{timestamp}] New customer added: {customer['name']}")

                elif operation == 'new_product':
                    product = create_product()
                    conn.execute(insert(products_table), [product])
                    print(f"[{timestamp}] New product added: {product['name']}")

                elif operation == 'new_order':
                    order = create_order(conn, customers_table, products_table)
                    conn.execute(insert(orders_table), [order])
                    print(f"[{timestamp}] New order placed: ${order['total']}")

                elif operation == 'update_stock':
                    product_update = update_product_stock(conn, products_table)
                    conn.execute(update(products_table).where(
                        products_table.c.id == product_update['id']
                    ).values(stock=product_update['stock']))
                    print(
                        f"[{timestamp}] Product stock updated: ID {product_update['id']} -> {product_update['stock']}")

            time.sleep(random.uniform(10, 11))

    except KeyboardInterrupt:
        print("\nSimulation stopped")


def main():
    parser = argparse.ArgumentParser(description='Database population script')
    parser.add_argument('--bulk', type=int, help='Bulk insert N rows per table and exit')
    args = parser.parse_args()

    engine = create_engine(build_db_url('postgresql'), echo=False, future=True)
    metadata = MetaData()

    customers_table = Table(
        "customers", metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String, nullable=False),
        Column("email", String, nullable=False),
        Column("created_at", TIMESTAMP)
    )

    products_table = Table(
        "products", metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String, nullable=False),
        Column("price", Numeric(10, 2)),
        Column("stock", Integer)
    )

    orders_table = Table(
        "orders", metadata,
        Column("id", Integer, primary_key=True),
        Column("customer_id", Integer, ForeignKey("customers.id")),
        Column("product_id", Integer, ForeignKey("products.id")),
        Column("quantity", Integer),
        Column("total", Numeric(10, 2)),
        Column("ordered_at", TIMESTAMP)
    )

    metadata.create_all(engine)

    if args.bulk:
        tables = [customers_table, products_table, orders_table]
        bulk_insert(engine, tables, args.bulk)
        return

    run_continuous_simulation(engine, customers_table, products_table, orders_table)


if __name__ == "__main__":
    main()
