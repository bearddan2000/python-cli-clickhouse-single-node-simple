import clickhouse_connect
import sys
import json

CLICKHOUSE_CLOUD_HOSTNAME = 'db'
CLICKHOUSE_CLOUD_USER = 'default'
CLICKHOUSE_CLOUD_PASSWORD = ''

client = clickhouse_connect.get_client(
    host=CLICKHOUSE_CLOUD_HOSTNAME, port=8123, username=CLICKHOUSE_CLOUD_USER, password=CLICKHOUSE_CLOUD_PASSWORD)

print("connected to " + CLICKHOUSE_CLOUD_HOSTNAME + "\n")
client.command(
    'CREATE TABLE IF NOT EXISTS dog (id UInt32, breed String, color String) ENGINE MergeTree ORDER BY id')

print("table dog created or exists already!\n")

row1 = [1, 'Lab', 'Black']
row2 = [2, 'Poodle', "White"]
row3 = [3, 'Mutt', "Mix"]
data = [row1, row2, row3]
client.insert('dog', data, column_names=['id', 'breed', 'color'])

print("written 3 rows to table dog\n")

QUERY = "SELECT * FROM dog"

result = client.query(QUERY)

sys.stdout.write("query: ["+QUERY + "] returns:\n\n")
print(result.result_rows)