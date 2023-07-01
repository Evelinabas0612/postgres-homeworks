import json

import psycopg2
from psycopg2 import sql


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True

    cur = conn.cursor()

    cur.execute(f'SELECT 1 FROM pg_catalog.pg_database WHERE datname=\'{db_name}\'')
    result = cur.fetchone()

    if result:
        cur.execute(f'DROP DATABASE {db_name}')
    cur.execute(f"""CREATE DATABASE {db_name};

                """)
    cur.close()
    conn.close()

    conn2 = psycopg2.connect(dbname=db_name, **params)

    with conn2.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS order_details;
            DROP TABLE IF EXISTS orders;
            DROP TABLE IF EXISTS customers;
            DROP TABLE IF EXISTS products;
            DROP TABLE IF EXISTS shippers;
            DROP TABLE IF EXISTS categories;
            DROP TABLE IF EXISTS region;
            DROP TABLE IF EXISTS employees;
           """)

    with conn2.cursor() as cur:
        cur.execute("""
               CREATE TABLE categories (
    category_id smallint NOT NULL,
    category_name character varying(15) NOT NULL,
    description text,
    picture bytea
);


--
-- Name: customers; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE customers (
    customer_id bpchar NOT NULL,
    company_name character varying(40) NOT NULL,
    contact_name character varying(30),
    contact_title character varying(30),
    address character varying(60),
    city character varying(15),
    region character varying(15),
    postal_code character varying(10),
    country character varying(15),
    phone character varying(24),
    fax character varying(24)
);


--
-- Name: employees; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE employees (
    employee_id smallint NOT NULL,
    last_name character varying(20) NOT NULL,
    first_name character varying(10) NOT NULL,
    title character varying(30),
    title_of_courtesy character varying(25),
    birth_date date,
    hire_date date,
    address character varying(60),
    city character varying(15),
    region character varying(15),
    postal_code character varying(10),
    country character varying(15),
    home_phone character varying(24),
    extension character varying(4),
    photo bytea,
    notes text,
    reports_to smallint,
    photo_path character varying(255)
);


--
-- Name: order_details; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE order_details (
    order_id smallint NOT NULL,
    product_id smallint NOT NULL,
    unit_price real NOT NULL,
    quantity smallint NOT NULL,
    discount real NOT NULL
);


--
-- Name: orders; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE orders (
    order_id smallint NOT NULL,
    customer_id bpchar,
    employee_id smallint,
    order_date date,
    required_date date,
    shipped_date date,
    ship_via smallint,
    freight real,
    ship_name character varying(40),
    ship_address character varying(60),
    ship_city character varying(15),
    ship_region character varying(15),
    ship_postal_code character varying(10),
    ship_country character varying(15)
);


--
-- Name: products; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE products (
    product_id smallint NOT NULL,
    product_name character varying(40) NOT NULL,
    category_id smallint,
    quantity_per_unit character varying(20),
    unit_price real,
    units_in_stock smallint,
    units_on_order smallint,
    reorder_level smallint,
    discontinued integer NOT NULL
);


--
-- Name: shippers; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE shippers (
    shipper_id smallint NOT NULL,
    company_name character varying(40) NOT NULL,
    phone character varying(24)
);
           """)

    conn2.commit()
    conn2.close()


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    with open(script_file, 'r', encoding='utf-8') as file:
        for line in file.readlines():
            cur.execute(f'{line};')
    cur.execute("""
    ALTER TABLE ONLY categories
    ADD CONSTRAINT pk_categories PRIMARY KEY (category_id);

    ALTER TABLE ONLY customers
    ADD CONSTRAINT pk_customers PRIMARY KEY (customer_id);

    ALTER TABLE ONLY employees
    ADD CONSTRAINT pk_employees PRIMARY KEY (employee_id);

    ALTER TABLE ONLY order_details
    ADD CONSTRAINT pk_order_details PRIMARY KEY (order_id, product_id);

    ALTER TABLE ONLY orders
    ADD CONSTRAINT pk_orders PRIMARY KEY (order_id);

    ALTER TABLE ONLY products
    ADD CONSTRAINT pk_products PRIMARY KEY (product_id);

    ALTER TABLE ONLY shippers
    ADD CONSTRAINT pk_shippers PRIMARY KEY (shipper_id);

    ALTER TABLE ONLY orders
    ADD CONSTRAINT fk_orders_customers FOREIGN KEY (customer_id) REFERENCES customers;

    ALTER TABLE ONLY orders
    ADD CONSTRAINT fk_orders_employees FOREIGN KEY (employee_id) REFERENCES employees;

    ALTER TABLE ONLY orders
    ADD CONSTRAINT fk_orders_shippers FOREIGN KEY (ship_via) REFERENCES shippers;

    ALTER TABLE ONLY order_details
    ADD CONSTRAINT fk_order_details_products FOREIGN KEY (product_id) REFERENCES products;

    ALTER TABLE ONLY order_details
    ADD CONSTRAINT fk_order_details_orders FOREIGN KEY (order_id) REFERENCES orders;

    ALTER TABLE ONLY products
    ADD CONSTRAINT fk_products_categories FOREIGN KEY (category_id) REFERENCES categories;

    ALTER TABLE ONLY employees
    ADD CONSTRAINT fk_employees_employees FOREIGN KEY (reports_to) REFERENCES employees;
    """)


def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""

    cur.execute("""
                DROP TABLE IF EXISTS suppliers;
                CREATE TABLE suppliers (
                supplier_id serial PRIMARY KEY,
                company_name character varying(100) NOT NULL,
                contact character varying(100) NOT NULL,
                address character varying(100) NOT NULL,
                phone character varying(20),
                fax character varying(20),
                homepage character varying(100),
                products text
                );""")


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    try:
        with open(json_file) as file:
            data = json.load(file)
            # print(data)
            return data
    except Exception as e:
        return 'Проблема с получением данных: ' + e


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    for row in suppliers:
        cur.execute(
            "INSERT INTO suppliers (company_name, contact, address, phone, fax, homepage, products) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING supplier_id",
            (row['company_name'], row['contact'], row['address'], row['phone'], row['fax'], row['homepage'],
             row['products'])
        )



def add_foreign_keys(cur, json_file) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    cur.execute(f"""
    ALTER TABLE products
    ADD CONSTRAINT fk_products_suppliers
    FOREIGN KEY (supplier_id)
    REFERENCES suppliers(supplier_id)
    ON DELETE CASCADE;"""
                    )


