from redis.commands.search.query import Query
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.aggregation import AggregateRequest, Asc, Desc
from time import sleep
from redis.commands.json.path import Path
from db_config import get_redis_connection
import json
import pprint
import requests
from RedisUtils import InsultDB

if __name__ == "__main__":
    # Declarations
    volume = 25
    search_user = "Reddit"

    # Instatantiate
    app = InsultDB()
    
    # Set Parameters
    app.set_redis_connection()
    app.define_schema()
    app.set_volume_desired(volume)
    app.search_single_user(search_user)

    # Retrieve data from API
    app.get_data()

    # 3 Basic Queries
    app.query_data()




