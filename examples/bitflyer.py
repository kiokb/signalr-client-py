from multiprocessing import Process, Queue


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

def signalr_listener(queue):
    from requests import Session
    from signalr import Connection
    
    with Session() as session:
        connection = Connection("https://signal.bitflyer.jp/signalr", session)
        connection.params = {"account_id": "DEMO2", "products": "FX_BTC_JPY,heartbeat"}
        listener = connection.register_hub('BFEXHub')

        def message_handler(messages):
            logs = get_executions(messages)
            if logs:
                queue.put(logs)
        
        listener.client.on('ReceiveTickers', message_handler)
        with connection:
            while True:
                connection.wait(120)
    

if __name__ == "__main__":
    queue = Queue()
    proc = Process(target=signalr_listener, args=(queue,))
    proc.start()

    while True:
        logs = queue.get()
        for log in logs:
            print(log)
