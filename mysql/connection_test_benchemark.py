#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import pymysql

def timer(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        return result, elapsed_time
    return wrapper

@timer
def test_ping(unix_socket, user, password, database, num_iterations):
    try:
        connection = pymysql.connect(unix_socket=unix_socket, user=user, password=password, database=database,
            charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        for _ in range(num_iterations):
            connection.ping(reconnect=False)
    except pymysql.MySQLError as e:
        print(f"Error during ping: {e}")
    finally:
        if connection:
            connection.close()

@timer
def test_select(unix_socket, user, password, database, num_iterations, sql):
    try:
        connection = pymysql.connect(unix_socket=unix_socket, user=user, password=password, database=database,
            charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        with connection.cursor() as cursor:
            for _ in range(num_iterations):
                cursor.execute(sql)
    except pymysql.MySQLError as e:
        print(f"Error during {sql}: {e}")
    finally:
        if connection:
            connection.close()

unix_socket = "/data/mysql/3306/data/mysql.sock"
user = "root"
password = "123456"
database = "information_schema"
num_iterations = 100000  # 执行次数

# 测试 PING 操作
result, elapsed_time = test_ping(unix_socket, user, password, database, num_iterations)
print(f"PING time for {num_iterations} iterations: {elapsed_time:.5f} seconds")

# 测试 SELECT 1
result, elapsed_time = test_select(unix_socket, user, password, database, num_iterations, "SELECT 1")
print(f"SELECT 1 time for {num_iterations} iterations: {elapsed_time:.5f} seconds")

# 测试 SHOW FULL TABLES FROM `information_schema` LIKE 'PROBABLYNOT'
result, elapsed_time = test_select(unix_socket, user, password, database, num_iterations, "SHOW FULL TABLES FROM `information_schema` LIKE 'PROBABLYNOT'")
print(f"SHOW FULL TABLES time for {num_iterations} iterations: {elapsed_time:.5f} seconds")

# 测试 INFORMATION_SCHEMA.TABLES 
new_get_tables_sql = "SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, CASE WHEN TABLE_TYPE='BASE TABLE' THEN CASE WHEN TABLE_SCHEMA = 'mysql' OR TABLE_SCHEMA = 'performance_schema' THEN 'SYSTEM TABLE' ELSE 'TABLE' END WHEN TABLE_TYPE='TEMPORARY' THEN 'LOCAL_TEMPORARY' ELSE TABLE_TYPE END AS TABLE_TYPE, TABLE_COMMENT AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, NULL AS SELF_REFERENCING_COL_NAME, NULL AS REF_GENERATION FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'PROBABLYNOT' HAVING TABLE_TYPE IN ('TABLE',null,null,null,null) ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME"
result, elapsed_time = test_select(unix_socket, user, password, database, num_iterations, new_get_tables_sql)
print(f"INFORMATION_SCHEMA.TABLES time for {num_iterations} iterations: {elapsed_time:.5f} seconds")
