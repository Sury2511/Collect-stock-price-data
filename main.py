import threading
import paho.mqtt.client as mqtt
import time
import requests
import parse as pr
import pymysql

class MQTTThread(threading.Thread):
    def __init__(self, server, port, path, client_id, symbol_list):
        super().__init__()
        self.server = server
        self.port = port
        self.path = path
        self.client_id = client_id
        self.symbol_list = symbol_list
        self.client = None
        self.received_data = []
        self.count = 0
        self.start_time = time.time()

    def run(self):
        self.client = mqtt.Client(client_id=self.client_id, transport="websockets")
        self.client.tls_set()  # Set TLS. Adjust this if you have specific SSL settings or certificates
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.ws_set_options(path=self.path)  # Set WebSocket path

        # Attempt to connect and start the loop
        self.reconnect_client()

    def reconnect_client(self):
        while True:
            try:
                self.client.connect(self.server, self.port, 60)
                self.client.loop_forever()
            except Exception as e:
                print(f"Error occurred: {e}")
              

    def on_connect(self, client, userdata, flags, rc):
        self.subscribe_to_next_symbol()

    def subscribe_to_next_symbol(self):
        symbol = self.symbol_list[self.count]
        self.client.subscribe(f"quotes/stock/SI/{symbol}", qos=1)
        self.client.subscribe(f"quotes/stock/TP/{symbol}", qos=1)

    def on_message(self, client, userdata, message):
        # ... (implement your message handling logic here)
        symbol = message.topic.split("/")[-1]
        default_data = {
            "Code": symbol,
            "Tradingtime": 0,
            "FBuyVol": 0.0,
            "FSellVol": 0.0,
            "matchPrice": 0.0,
            "MatchedTotalVol": 0.0,
            "MatchedChange": 0.0,
            "RefPrice": 0.0,
            "BidPrice1": 0,
            "BidPrice2": 0,
            "BidPrice3": 0,
            "BidVol1": 0,
            "BidVol2": 0,
            "BidVol3": 0,
            "OfferPrice1": 0,
            "OfferPrice2": 0,
            "OfferPrice3": 0,
            "OfferVol1": 0,
            "OfferVol2": 0,
            "OfferVol3": 0
        }
        data_dict = self.find_data_for_symbol(symbol)

        if not data_dict:
            data_dict = default_data.copy()
            data_dict["Code"] = symbol
            self.received_data.append(data_dict)

        if "quotes/stock/SI/" in message.topic:
            stock_info_data = pr.deserialize_stock_info(message.payload)
            data_dict.update({
                "Tradingtime": stock_info_data.tradingTime.seconds,
                "FBuyVol": stock_info_data.buyForeignQtty,
                "FSellVol": stock_info_data.sellForeignQtty,
                "matchPrice": stock_info_data.matchPrice,
                "MatchedTotalVol": stock_info_data.matchQtty,
                "MatchedChange": stock_info_data.changed,
                "RefPrice": stock_info_data.estimatedPrice
            })

        elif "quotes/stock/TP/" in message.topic:
            top_price_data = pr.deserialize_top_price(message.payload)
            
            def get_data(data_list, index, attribute):
                try:
                    return getattr(data_list[index], attribute)
                except IndexError:
                    return 0
                    
            data_dict.update({
                "BidPrice1": get_data(top_price_data.bid, 0, 'price'),
                "BidPrice2": get_data(top_price_data.bid, 1, 'price'),
                "BidPrice3": get_data(top_price_data.bid, 2, 'price'),
                "BidVol1": get_data(top_price_data.bid, 0, 'qtty'),
                "BidVol2": get_data(top_price_data.bid, 1, 'qtty'),
                "BidVol3": get_data(top_price_data.bid, 2, 'qtty'),
                "OfferPrice1": get_data(top_price_data.ask, 0, 'price'),
                "OfferPrice2": get_data(top_price_data.ask, 1, 'price'),
                "OfferPrice3": get_data(top_price_data.ask, 2, 'price'),
                "OfferVol1": get_data(top_price_data.ask, 0, 'qtty'),
                "OfferVol2": get_data(top_price_data.ask, 1, 'qtty'),
                "OfferVol3": get_data(top_price_data.ask, 2, 'qtty')
            })

        # If both SI and TP messages have been received for the current symbol
        if self.check_both_SI_and_TP_received_for_symbol(symbol):
            # Unsubscribe from the current symbol's topics
            current_symbol = self.symbol_list[self.count]
            client.unsubscribe(f"quotes/stock/SI/{current_symbol}")
            client.unsubscribe(f"quotes/stock/TP/{current_symbol}")
            self.count += 1
            if self.count < len(self.symbol_list):
                self.subscribe_to_next_symbol()
            else:
                # Finished processing all symbols
                elapsed_time = time.time() - self.start_time
                print(f"Processed all symbols in {elapsed_time:.2f} seconds.")
                # Reset the count and start time, clear received data, and start over
                self.count = 0
                self.start_time = time.time()
                self.write_data_to_mysql(self.received_data)
                self.received_data.clear()
                self.subscribe_to_next_symbol()

    def check_both_SI_and_TP_received_for_symbol(self, symbol):
        # Implement your logic here
        return True

    def find_data_for_symbol(self, symbol):
        for data in self.received_data:
            if data["Code"] == symbol:
                return data
        return None
    
    def write_data_to_mysql(self, data):
        conn = pymysql.connect(host='127.0.0.1', user='tuan', password='sieunhan2511', db='TESTER', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        try:
            with conn.cursor() as cursor:
                # Loop through each data entry and insert it into the appropriate table
                for entry in data:
                    # Determine the table name by the first letter of the stock symbol
                    table_name_suffix = entry['Code'][0].lower() if entry['Code'][0].isalpha() else 'other'
                    table_name = f"orderbook_{table_name_suffix}"

                    cursor.execute(f"SELECT Tradingtime, MatchedTotalVol FROM `{table_name}` WHERE Code=%s ORDER BY Tradingtime DESC LIMIT 1", (entry['Code'],))
                    row = cursor.fetchone()

                    if row and entry['MatchedTotalVol'] <= row['MatchedTotalVol']: 
                        continue

                    sql = f"""INSERT INTO `{table_name}` (Code, Tradingtime, FBuyVol, FSellVol, matchPrice, MatchedTotalVol, MatchedChange, RefPrice, BidPrice1, BidPrice2, BidPrice3, BidVol1, BidVol2, BidVol3, OfferPrice1, OfferPrice2, OfferPrice3, OfferVol1, OfferVol2, OfferVol3)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                            Tradingtime=VALUES(Tradingtime),
                            FBuyVol=VALUES(FBuyVol),
                            FSellVol=VALUES(FSellVol),
                            matchPrice=VALUES(matchPrice),
                            MatchedTotalVol=VALUES(MatchedTotalVol),
                            MatchedChange=VALUES(MatchedChange),
                            RefPrice=VALUES(RefPrice),
                            BidPrice1=VALUES(BidPrice1),
                            BidPrice2=VALUES(BidPrice2),
                            BidPrice3=VALUES(BidPrice3),
                            BidVol1=VALUES(BidVol1),
                            BidVol2=VALUES(BidVol2),
                            BidVol3=VALUES(BidVol3),
                            OfferPrice1=VALUES(OfferPrice1),
                            OfferPrice2=VALUES(OfferPrice2),
                            OfferPrice3=VALUES(OfferPrice3),
                            OfferVol1=VALUES(OfferVol1),
                            OfferVol2=VALUES(OfferVol2),
                            OfferVol3=VALUES(OfferVol3)
                        """

                    # Execute the SQL statement with data
                    cursor.execute(sql, (
                        entry['Code'],
                        entry['Tradingtime'],
                        entry['FBuyVol'],
                        entry['FSellVol'],
                        entry['matchPrice'],
                        entry['MatchedTotalVol'],
                        entry['MatchedChange'],
                        entry['RefPrice'],
                        entry['BidPrice1'],
                        entry['BidPrice2'],
                        entry['BidPrice3'],
                        entry['BidVol1'],
                        entry['BidVol2'],
                        entry['BidVol3'],
                        entry['OfferPrice1'],
                        entry['OfferPrice2'],
                        entry['OfferPrice3'],
                        entry['OfferVol1'],
                        entry['OfferVol2'],
                        entry['OfferVol3']
                    ))

            # Commit changes
            conn.commit()
        except Exception as e:
            print(f"An error occurred while trying to write to the database: {e}")
        finally:
            # Close the connection whether or not data was successfully written
            conn.close()


symbol_all = requests.get("https://tradeapi.bsc.com.vn/trade/quotes?symbols=ALL").json()
stock_symbol = [item['symbol'] for item in symbol_all['d']]
stock_symbol = sorted(stock_symbol) 

# Start the MQTT thread

mqtt_thread = MQTTThread(server="datafeed.dnse.com.vn", port=443, path="/wss",
                         client_id="064C246644", symbol_list=stock_symbol)
while True:
    try:
        if not mqtt_thread.is_alive():
            mqtt_thread.start()

    except Exception as e:
        print(f"An error occurred: {e}")
        time.sleep(5)  # wait a bit before retrying or continuing

