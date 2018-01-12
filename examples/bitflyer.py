from requests import Session
from signalr import Connection
from time import sleep
import pprint
import sys

def get_executions(messages):
    logs = []
    for d in messages:
        if "tk" in d:
            tk = d["tk"]
            if tk["tis"] is not None:
                log = {}
                log["exec_date"] = d["ts"]
                log["side"] = tk["tis"]
                log["size"] = tk["tv"]
                log["price"] = tk["LTP"]
                logs.append(log)
    return logs

if __name__ == "__main__":
    with Session() as session:
        connection = Connection("https://lightning.bitflyer.jp/signalr", session)
        connection.params = {"account_id": "DEMO2", "products": "FX_BTC_JPY,heartbeat"}
        chat = connection.register_hub('BFEXHub')

        def print_received_message(messages):
            logs = get_executions(messages)
            for log in logs:
                print(log)

        def print_error(error):
            print('error: ', error)

        chat.client.on('ReceiveTickers', print_received_message)

        connection.error += print_error

        with connection:
            while True:
                connection.wait(120)
