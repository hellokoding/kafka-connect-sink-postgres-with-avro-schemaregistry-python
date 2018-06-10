from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import time


value_schema_str = """
{
   "namespace": "hellokoding.kafka",
   "name": "value",
   "type": "record",
   "fields" : [
     {"name" : "browser", "type" : "string"},
     {"name" : "created_at", "type" : "int", "logicalType": "date"}
   ]
}
"""

key_schema_str = """
{
   "namespace": "hellokoding.kafka",
   "name": "key",
   "type": "record",
   "fields" : [
     {"name" : "url", "type" : "string"}
   ]
}
"""

value_schema = avro.loads(value_schema_str)
key_schema = avro.loads(key_schema_str)
value = {"browser": "Chrome", "created_at": int(time.time())}
key = {"url": "http://localhost:8081"}

avroProducer = AvroProducer({
    'bootstrap.servers': 'localhost:9092',
    'schema.registry.url': 'http://localhost:8081'
    }, default_key_schema=key_schema, default_value_schema=value_schema)

avroProducer.produce(topic='page_1', value=value, key=key)
avroProducer.flush()
