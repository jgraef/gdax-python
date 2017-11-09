# gdax/WebsocketClient.py
# original author: Daniel Paquin
# mongo "support" added by Drew Rice
# rewritten by Janosch Gräf
#
# Template object to receive messages from the gdax Websocket Feed

from __future__ import print_function
import json
import base64
import hmac
import hashlib
import time
from threading import Thread
from websocket import create_connection, WebSocketConnectionClosedException



class WebsocketListener(object):
    def on_message(self, msg):
        pass

    def on_heartbeat(self, msg):
        pass

    def on_ticker(self, msg):
        pass

    def on_snapshot(self, msg):
        pass

    def on_l2update(self, msg):
        pass

    def on_received(self, msg):
        pass

    def on_open(self, msg):
        pass

    def on_done(self, msg):
        pass

    def on_match(self, msg):
        pass

    def on_change(self, msg):
        pass

    def on_margin_profile_update(self, msg):
        pass

    def on_activate(self, msg):
        pass

    def on_connect(self):
        pass

    def on_disconnect(self):
        pass


class WebsocketClient(object):
    def __init__(self, listener = None, url="wss://ws-feed.gdax.com", auth=False, api_key="",
                 api_secret="", api_passphrase=""):

        self.listeners = set()
        if listener is not None:
            self.listeners.add(listener)
        self.url = url
        self._stop = False
        self.error = None
        self._ws = None
        self._thread = None
        self.auth = auth
        self.api_key = api_key
        self.api_secret = base64.b64decode(api_secret)
        self.api_passphrase = api_passphrase
        self.last_ping = 0
        self._connect()

    def _connect(self):
        self._ws = create_connection(self.url)
        self.on_open()

        def _go():
            self._listen()
            self._disconnect()

        self._stop = False
        self._thread = Thread(target=_go)
        self._thread.start()

    def _listen(self):
        while not self._stop:
            # NOTE: This should be replaced with time.monotonic, since time.time() can run backwards. On the other
            #       hand, time.monotonic is only guaranteed to be available since Python 3.5
            t = time.time()
            if t - self.last_ping > 30:
                # Set a 30 second ping to keep connection alive
                self._ws.ping("keepalive")
                self.last_ping = t

            data = self._ws.recv()
            if not data:
                # socket closed
                break

            try:
                msg = json.loads(data)
            except ValueError as e:
                # JSON deserialization error
                self.on_error(e)
            else:
                self.on_message(msg)

    def _disconnect(self):
        if self._ws:
            self._ws.close()
        self.on_close()

    def close(self):
        self._stop = True
        self._thread.join()

    def on_open(self):
        for listener in self.listeners:
            listener.on_connect()

    def on_close(self):
        for listener in self.listeners:
            listener.on_disconnect()

    def on_message(self, msg):
        for listener in self.listeners:
            listener.on_message(msg)
            method = getattr(listener, "on_" + msg["type"], None)
            if callable(method):
                method(msg)

    def on_error(self, e, data=None):
        self.error = e
        self._stop = True

    def sign_message(self, msg):
        timestamp = str(int(time.time()))
        message = "{!s}GET/users/self/verify".format(timestamp)
        signature = hmac.new(self.api_secret, message.encode(), hashlib.sha256)
        msg["signature"] = base64.b64encode(signature.digest()).decode()
        msg["key"] = self.api_key
        msg["passphrase"] = self.api_passphrase
        msg["timestamp"] = timestamp

    def send_message(self, msg, sign=False):
        if self._ws is None:
            raise ConnectionError("Not connected")
        if self.auth and sign:
            self.sign_message(msg)
        self._ws.send(json.dumps(msg))

    def subscribe(self, name, products):
        self.send_message({
            "type": "subscribe",
            "channels": [{
                "name": name,
                "product_ids": products
            }]
        }, sign=True)

    def add_listener(self, listener):
        self.listeners.add(listener)

    def remove_listener(self, listener):
        self.listeners.remove(listener)


if __name__ == "__main__":
    import sys
    import gdax
    import time

    class MyWebsocketClient(gdax.WebsocketClient):
        message_count = 0

        def on_open(self):
            self.message_count = 0
            print("Let's count the messages!")

        def on_message(self, msg):
            print(json.dumps(msg, indent=4, sort_keys=True))
            self.message_count += 1

        def on_close(self):
            print("-- Goodbye! --")

    wsClient = MyWebsocketClient()
    wsClient.subscribe("ticker", ["BTC-EUR"])

    try:
        while True:
            print("\nMessageCount =", "%i \n" % wsClient.message_count)
            time.sleep(1)
    except KeyboardInterrupt:
        wsClient.close()

    if wsClient.error:
        sys.exit(1)
    else:
        sys.exit(0)
