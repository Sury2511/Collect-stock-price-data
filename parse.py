import models_pb2
from price import price_pb2
import requests

def deserialize_stock_info(data: str) -> price_pb2.StockInfo:
    """
    Deserialize stock info
    Consume from topic stock info
    :param data: raw message
    :return: stock info model
    """
    encapMessage = models_pb2.EncapMessage()
    encapMessage.ParseFromString(data)
    if encapMessage.type == models_pb2.EncapMessage.STOCK_INFO:
        # parse stock info
        stock_info = price_pb2.StockInfo()
        stock_info.ParseFromString(encapMessage.payload)
        return stock_info


def deserialize_top_price(data: str) -> price_pb2.TopPrice:
    encapMessage = models_pb2.EncapMessage()
    encapMessage.ParseFromString(data)
    if encapMessage.type == models_pb2.EncapMessage.TOP_PRICE:
        # parse top price
        top_price = price_pb2.TopPrice()
        top_price.ParseFromString(encapMessage.payload)
        return top_price
    
