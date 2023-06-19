"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import psycopg2


def get_data_from_file(path_name_file):
    """
    Получение данных из файла
    """
    try:
        file = open(path_name_file, 'r')
        data = csv.DictReader(file)
        return data
    except Exception as e:
        return 'Проблема с получением данных: ' + e


conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='root')
try:
    with conn:
        with conn.cursor() as cur_one:
            # Добавление данных из файла customers_data в таблицу "customers"
            data = get_data_from_file('north_data/customers_data.csv')
            for row in data:
                cur_one.execute(
                    "INSERT INTO customers (customer_id, company_name, contact_name) VALUES (%s, %s, %s)",
                    (row['customer_id'], row['company_name'], row['contact_name'])
                )
        with conn.cursor() as cur_two:
            # Добавление данных из файла employees_data.csv в таблицу "employees"
            data = get_data_from_file('north_data/employees_data.csv')
            for row in data:
                cur_two.execute(
                    "INSERT INTO employees (employee_id, first_name, last_name, title, birth_date, notes) VALUES (%s, %s, %s, %s, %s, %s)",
                    (row['employee_id'], row['first_name'], row['last_name'], row['title'], row['birth_date'], row['notes'])
                )
        with conn.cursor() as cur_three:
            # Добавление данных из файла orders_data.csv в таблицу "orders"
            data = get_data_from_file('north_data/orders_data.csv')
            for row in data:
                cur_three.execute(
                    "INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_city) VALUES (%s, %s, %s, %s, %s)",
                    (row['order_id'], row['customer_id'], row['employee_id'], row['order_date'], row['ship_city'])
                )
finally:
    conn.close()
