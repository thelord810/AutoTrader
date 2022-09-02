from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer
import asyncio

from aiokafka import AIOKafkaConsumer
import asyncio
import json
import pandas as pd

marketdata = pd.DataFrame(columns=['symbol', 'open', 'last', 'high', 'low', 'change', 'bPrice', 'bQty', 'sPrice', 'sQty', 'ltq', 'avgPrice', 'quotes', 'OI', 'CHNGOI', 'ttq', 'totalBuyQt', 'totalSellQ', 'ttv', 'trend', 'lowerCktLm', 'upperCktLm', 'ltt', 'close', 'exchange'])
schema = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Data",
  "description": "A Confluent Kafka Market Data Schena",
  "type": "object",
  "properties": {
    "symbol": {
      "description": "User's favorite number",
      "type": "string"
    },
    "open": {
      "description": "User's favorite color",
      "type": "number"
    },
    "last": {
      "description": "User's twitter handle",
      "type": "number"
    },
    "high": {
      "description": "Just to demo the issue with required fields",
      "type": "number"
    },
    "low": {
      "description": "Just to demo the issue with required fields",
      "type": "number"
    }
  },
  "required": ["open", "last", "high", "low"]
}
"""


def dict_to_data(obj, ctx):
    if obj is None:
        return None

    return dict(open=obj['open'],
                last=obj['last'],
                high=obj['high'],
                low=obj['low'])
#
#
# async def consume_json():
#     json_deserializer = JSONDeserializer(schema, from_dict=dict_to_data)
#     string_deserializer = StringDeserializer('utf_8')
#     # https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html
#     consumer = DeserializingConsumer({
#         'bootstrap.servers': 'localhost:9092',
#         'key.deserializer': string_deserializer,
#         'value.deserializer': json_deserializer,
#         'group.id': 'json-consumer-group-1',
#         'auto.offset.reset': 'earliest'
#     })
#     consumer.subscribe(['ticker'])
#     global marketdata
#     while True:
#         try:
#             msg = consumer.poll(1.0)
#             if msg is None:
#                 continue
#
#             m_data = msg.value()
#             #marketdata = marketdata.append(m_data, ignore_index = True)
#             await asyncio.sleep(1)
#             # if m_data is not None:
#             #     print(f'User name: {m_data.open}, '
#             #           f'favorite number:{m_data.last}, '
#             #           f'favorite color:{m_data.high}, '
#             #           f'twitter handle:{m_data.low}')
#         except KeyboardInterrupt:
#             break
#
#     print('closing the consumer')
#     consumer.close()

json_deserializer = JSONDeserializer(schema, from_dict=dict_to_data)
string_deserializer = StringDeserializer('utf_8')
# https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html
consumer = DeserializingConsumer({
    'bootstrap.servers': 'localhost:9092',
    'key.deserializer': string_deserializer,
    'value.deserializer': json_deserializer,
    'group.id': 'json-consumer-group-1',
    'auto.offset.reset': 'earliest'
})
def consume(consumer, timeout):
    while True:
        message = consumer.poll(timeout)
        if message is None:
            continue
        if message.error():
            print("Consumer error: {}".format(message.error()))
            continue
        yield message
    consumer.close()

def confluent_consumer():
    consumer.subscribe(['ticker'])
    for msg in consume(consumer, 1.0):
        yield msg.value()