from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer
import multitasking
import pandas as pd
import signal

# kill all tasks on ctrl-c
signal.signal(signal.SIGINT, multitasking.killall)

marketdata = pd.DataFrame(columns=['symbol', 'open', 'last', 'high', 'low', 'change', 'bPrice', 'bQty', 'sPrice', 'sQty', 'ltq', 'avgPrice', 'quotes', 'OI', 'CHNGOI', 'ttq', 'totalBuyQt', 'totalSellQ', 'ttv', 'trend', 'lowerCktLm', 'upperCktLm', 'ltt', 'close', 'exchange'])
schema = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "IciciData",
  "description": "A Confluent Kafka Market Data Schena",
  "type": "object",
  "properties": {
    "symbol": {
      "description": "Stock Symbol Token",
      "type": "string"
    },
    "open": {
      "description": "Open Price",
      "type": "number"
    },
    "last": {
      "description": "Last Price",
      "type": "number"
    },
    "high": {
      "description": "High Price",
      "type": "number"
    },
    "low": {
      "description": "Low Price",
      "type": "number"
    },
    "change": {
      "description": "Price Change",
      "type": "number"
    },
    "bPrice": {
      "description": "Buy Price",
      "type": "number"
    },
    "bQty": {
      "description": "Buy Quantity",
      "type": "number"
    },
    "sPrice": {
      "description": "Sell Price",
      "type": "number"
    },
    "sQty": {
      "description": "Sell Quantity",
      "type": "number"
    },
    "ltq": {
      "description": "Last Traded Quantity",
      "type": "number"
    },
    "avgPrice": {
      "description": "Average Price",
      "type": "number"
    },       
    "quotes": {
      "description": "Quotes",
      "type": "string"
    },  
    "OI": {
      "description": "Open Interest",
      "type": "number"
    },  
    "CHNGOI": {
      "description": "Change in OI",
      "type": "string"
    },  
    "ttq": {
      "description": "Total Traded Quantity",
      "type": "number"
    },  
    "totalBuyQt": {
      "description": "Total Sell Quantity",
      "type": "number"
    },  
    "totalSellQ": {
      "description": "Total Sell Quantity",
      "type": "number"
    },  
    "ttv": {
      "description": "Total Traded volume",
      "type": "string"
    },  
    "trend": {
      "description": "Trend",
      "type": "number"
    },  
    "lowerCktLm": {
      "description": "Lower Circuit limit",
      "type": "number"
    },  
    "upperCktLm": {
      "description": "Upper Circuit limit",
      "type": "number"
    },  
    "ltt": {
      "description": "Last traded time",
      "type": "string"
    },  
    "close": {
      "description": "Last day close price",
      "type": "number"
    },  
    "exchange": {
      "description": "Exchange",
      "type": "string"
    },     
    "product_type": {
      "description": "Product Type",
      "type": "string"
    },     
    "expiry_date": {
      "description": "Expiry date of Instrument",
      "type": "string"
    }                                                                    
  },
  "required": ["open", "last", "high", "low", "change", "bPrice","bQty","sPrice","sQty","ltq","avgPrice","OI","CHNGOI","ttq","totalBuyQt","totalSellQ","ttv","lowerCktLm","upperCktLm","ltt","close","product_type", "expiry_date","symbol"]
}
"""

def dict_to_data(obj, ctx):
    if obj is None:
        return None

    return dict(open=obj['open'],
                last=obj['last'],
                high=obj['high'],
                low=obj['low'],
                bPrice=obj['bPrice'],
                bQty=obj['bQty'],
                sPrice=obj['sPrice'],
                sQty=obj['sQty'],
                OI=obj['OI'],
                ttq=obj['ttq'],
                ttv=obj['ttv'],
                ltt=obj['ltt'],
                ltq=obj['ltq'],
                avgPrice=obj['avgPrice'],
                CHNGOI=obj['CHNGOI'],
                totalBuyQt=obj['totalBuyQt'],
                totalSellQ=obj['totalSellQ'],
                lowerCktLm=obj['lowerCktLm'],
                upperCktLm=obj['upperCktLm'],
                close=obj['close'],
                product_type=obj['product_type'],
                expiry_date=obj['expiry_date'],
                change=obj['change'],
                symbol=obj['symbol']
                )
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

@multitasking.task
def confluent_consumer(data_class):
    consumer.subscribe(['newticker'])
    data_class.data_dict = pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'volume', 'open_interest', 'count', 'symbol'])
    #data = pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'volume', 'open_interest', 'count', 'symbol'])
    for msg in consume(consumer, 1.0):
        row_data_df = pd.DataFrame([msg.value()])
        row_data_df.rename(
            columns={'open': 'Open', 'low': 'Low', 'last': 'Close', 'high': 'High', 'ttv': 'volume',
                     'OI': 'open_interest',
                     'ltt': 'datetime', 'close': 'previous_close'}, inplace=True)
        row_data_df['datetime'] = pd.to_datetime(row_data_df['datetime'], format='%a %b  %d %H:%M:%S %Y', utc=True)
        row_data_df.set_index('datetime', inplace=True)
        data_class.data_dict = pd.concat([data_class.data_dict, row_data_df])