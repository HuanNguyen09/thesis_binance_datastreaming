# import websocket
# import json

# # Function to handle trades
# def handle_trade(message):
#     trade = json.loads(message)
#     print(trade)

# # Connect to the Binance websocket for trade updates
# def connect_to_websocket(symbol):
#     ws_url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@trade"
#     websocket.enableTrace(True)
#     ws = websocket.WebSocketApp(ws_url, on_message=handle_trade)
#     ws.run_forever()

# # Main function
# def main():
#     symbol = 'BTCUSDT'  # Change this to the symbol you want to monitor
#     connect_to_websocket(symbol)

# if __name__ == "__main__":
#     main()

from kafka import KafkaProducer
import json
import websocket

# Initialize Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

from pyflink.common import Types
def handle_trade(ws, message):
    trade = json.loads(message)
    # Đảm bảo trade có cùng định dạng với schema của Kafka Consumer
    
    trade_formatted = { 
        "f0": trade.get("e", ""),
        "f1": trade.get("E", 0),
        "f2": trade.get("s", ""),
        "f3": trade.get("t", 0),
        "f4": trade.get("p", ""),
        "f5": trade.get("q", ""),
        "f6": trade.get("b", 0),
        "f7": trade.get("a", 0),
        "f8": trade.get("T", 0),
        "f9": trade.get("m", False),
        "f10": trade.get("M", False)
    }
    # Gửi dữ liệu theo định dạng đã chuẩn bị
    producer.send('test2', json.dumps(trade_formatted).encode('utf-8'))


# Connect to the Binance websocket for trade updates
def connect_to_websocket(symbol):
    ws_url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@trade"
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(ws_url, on_message=handle_trade)
    ws.run_forever()

# Main function
def main():
    symbol = 'BTCUSDT'  # Change this to the symbol you want to monitor
    connect_to_websocket(symbol)

if __name__ == "__main__":
    main()
