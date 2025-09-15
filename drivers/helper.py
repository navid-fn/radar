import requests
import websocket
import json


def on_message(ws, message):
    print("Trade event:", message)

def on_open(ws):
    ws.send(json.dumps({"method":"sub_to_market_data", "symbols":["USDT_IRT"]}))

def scrape():
    url = 'wss://ws.bitpin.ir'
    ws = websocket.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message
    )

    ws.run_forever()          



if __name__ == '__main__':
    scrape()

    
