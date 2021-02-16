import os
from dotenv import load_dotenv

load_dotenv()
print("hello")
print(os.getenv('INFLUX_ENDPOINT'))
print(os.getenv('INFLUXDB_DB_Weather'))
