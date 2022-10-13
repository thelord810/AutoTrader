from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer
import multitasking
import pandas as pd
import signal

# kill all tasks on ctrl-c
signal.signal(signal.SIGINT, multitasking.killall)

class KafkaConsumer:
    def __init__(
        self
    ) -> None:
        self.schema = """
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
        self.json_deserializer = JSONDeserializer(self.schema, from_dict=self.dict_to_data)
        self.string_deserializer = StringDeserializer('utf_8')
        # https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html
        self.consumer = DeserializingConsumer({
            'bootstrap.servers': 'localhost:9092',
            'key.deserializer': self.string_deserializer,
            'value.deserializer': self.json_deserializer,
            'group.id': 'json-consumer-group-1',
            'auto.offset.reset': 'earliest'
        })

    def dict_to_data(self, obj, ctx):
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

    def consume(self, consumer, timeout):
        while True:
            message = consumer.poll(timeout)
            if message is None:
                continue
            if message.error():
                print("Consumer error: {}".format(message.error()))
                continue
            yield message
        consumer.close()
