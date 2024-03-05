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

class InsultDB:
    def __init__(self):
        self.r = None
        self.url = "https://evilinsult.com/generate_insult.php"
        self.params = {"lang": "en", "type": "json"}
        self.volume_desired = 30            #Default
        self.index_name = 'idx:insult'
        self.rs = "None"
        self.user_search = "Martin Luther"  #Default

    def set_redis_connection(self):
        try:
            self.r = get_redis_connection()
        except:
            print("\n I didn't get the Redis Connection")
        
    def set_volume_desired(self, volume):
        if volume.is_integer():
            self.volume_desired = volume
        else:
            print("\nMethod only accepts integers")    
   
    def search_single_user(self, user_search):
        self.user_search = user_search

    def define_schema(self):
        try:
            self.rs = self.r.ft(self.index_name)
            self.rs.info(self.index_name)
        except:
            self.r.execute_command('FLUSHALL')
            schema = (
                TextField("$.number", as_name="number"), 
                TextField("$.language", as_name="language"),
                TextField("$.insult", as_name="insult"), 
                TextField("$.created", as_name="created"), 
                TextField("$.shown", as_name="shown"), 
                TextField("$.createdby", as_name="createdby"),
                TextField("$.active", as_name="active"), 
                TextField("$.comment", as_name="comment"),
            )

            self.rs = self.r.ft(self.index_name)
            self.rs.create_index(
                schema,
                definition=IndexDefinition(
                    prefix=["insult:"], index_type=IndexType.JSON
                )
            )

    def get_data(self):
        if self.r != None:
            for i in range(self.volume_desired):
                if i % 8 == 0:
                    print("Processing and pushing to Redis...")
                response = requests.get(self.url, self.params)
            # If successful, load the data into a string
                if response.status_code == 200:
                    data = json.loads(response.text)
                    keyname = '{}:{}'.format('insult', str(i + 1))
                    self.r.execute_command('JSON.SET', keyname, '.', json.dumps(data))
                else:
                    print(f"Error: {response.status_code}")
                    #Wait a little. Possibly rate limited.
                    sleep(5)
                    print("\nRetrying...")
                    if response.status_code == 200:
                        data = json.loads(response.text)
                        keyname = '{}:{}'.format('insult', str(i + 1))
                        self.r.execute_command('JSON.SET', keyname, '.', json.dumps(data))
                    else:
                        print(f"Error: {response.status_code}")
                        exit(1)
        else:
            print("\nError. Try to establish connection first.")
    
    def query_data(self):
        try:
            search_term = '@createdby: {}'.format(self.user_search)
            print("\nPosts by username " + self.user_search + "\n", self.rs.search(Query(search_term)))
            pprint.pprint("-" * 190)

            print("\nInsults relating to Mother\n", self.rs.search(Query("@insult: mom")))
            pprint.pprint("-" * 190) 

            print("\nData tagged as English\n", self.rs.search(Query("@language: en")))
            pprint.pprint("-" * 190)
        except:
            print("There was a problem creating the schema for querying")

            