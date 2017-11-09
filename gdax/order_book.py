#
# gdax/order_book.py
# David Caseria
#
# Live order book updated from the gdax Websocket Feed

import operator
from threading import RLock
from decimal import Decimal
import logging

from sortedcontainers import SortedDict

from gdax.public_client import PublicClient
from gdax.websocket_client import WebsocketListener


logging.basicConfig(
    level=logging.DEBUG
)

logger = logging.getLogger(__name__)


class OrderBook(WebsocketListener):
    def __init__(self, product_id, public_client=None):
        self._asks = SortedDict()
        self._bids = SortedDict(operator.neg)
        self._product_id = product_id
        self._client = public_client or PublicClient()
        self._sequence = -1
        self._current_ticker = None
        self._msg_queue = None
        self._lock = RLock()

    @property
    def product_id(self):
        ''' Currently OrderBook only supports a single product even though it is stored as a list of products. '''
        return self._product_id

    def reset_book(self):
        with self._lock:
            res = self._client.get_product_order_book(product_id=self.product_id, level=3)
            for bid in res['bids']:
                self._add({
                    'id': bid[2],
                    'side': 'buy',
                    'price': Decimal(bid[0]),
                    'size': Decimal(bid[1])
                })
            for ask in res['asks']:
                self._add({
                    'id': ask[2],
                    'side': 'sell',
                    'price': Decimal(ask[0]),
                    'size': Decimal(ask[1])
                })
            self._sequence = res['sequence']

    def on_message(self, message):
        try:
            sequence = message['sequence']
        except KeyError:
            return
        if self._sequence == -1:
            self.reset_book()
        if sequence <= self._sequence:
            pass # ignore older messages (e.g. before order book initialization from getProductOrderBook)
        elif sequence > self._sequence + 1:
            self.on_sequence_gap(self._sequence, sequence)
        else:
            msg_type = message['type']
            if msg_type == 'open':
                self._add(message)
                self.on_orderbook_change()
            elif msg_type == 'done' and 'price' in message:
                self._remove(message)
                self.on_orderbook_change()
            elif msg_type == 'match':
                self._match(message)
                self._current_ticker = message
                self.on_orderbook_change()
            elif msg_type == 'change':
                self._change(message)
                self.on_orderbook_change()
            self._sequence = sequence

    def on_orderbook_change(self):
        pass

    def on_sequence_gap(self, gap_start, gap_end):
        logger.warning('Error: messages missing ({} - {}). Re-initializing  book at sequence.'.format(
            gap_start, gap_end, self._sequence))
        self.reset_book()

    def _add(self, order):
        order = {
            'id': order.get('order_id') or order['id'],
            'side': order['side'],
            'price': Decimal(order['price']),
            'size': Decimal(order.get('size') or order['remaining_size'])
        }
        price = order["price"]
        side = self._bids if order['side'] == 'buy' else self._asks
        with self._lock:
            bids = side.get(price)
            if bids is None:
                side[price] = {order["id"]: order}
            else:
                bids[order["id"]] = order

    def _remove(self, order):
        id = order["order_id"]
        price = Decimal(order['price'])
        side = self._bids if order['side'] == 'buy' else self._asks
        with self._lock:
            bids = side.get(price)
            if bids is not None:
                del bids[id]
                if len(bids) == 0:
                    del side[price]

    def _match(self, order):
        size = Decimal(order['size'])
        price = Decimal(order['price'])
        side = self._bids if order['side'] == 'buy' else self._asks
        with self._lock:
            bids = side.get(price)
            if bids:
                bid = bids.get(order['maker_order_id'])
                if bid['size'] == size:
                    del bids[order['maker_order_id']]
                    if len(bids) == 0:
                        del side[price]
                else:
                    bid['size'] -= size

    def _change(self, order):
        try:
            new_size = Decimal(order['new_size'])
        except KeyError:
            return

        try:
            price = Decimal(order['price'])
        except KeyError:
            return

        side = self._bids if order['side'] == 'buy' else self._asks

        with self._lock:
            bids = side.get(price)
            bid = bids.get(order["order_id"])
            if bid is not None:
                bid['size'] = new_size

    def get_current_ticker(self):
        return self._current_ticker

    def get_current_book(self):
        result = {
            'sequence': self._sequence,
            'asks': [],
            'bids': [],
        }
        with self._lock:
            for price, asks in self._asks.items():
                for order in asks:
                    result['asks'].append([order['price'], order['size'], order['id']])
            for price, bids in self._bids.items():
                for order in bids:
                    result['bids'].append([order['price'], order['size'], order['id']])
        return result

    def get_ask(self):
        prices = self._asks.keys()
        return None if not prices else prices[0]

    def get_asks(self, price):
        return self._asks.get(price, {})

    def get_bid(self):
        prices = self._bids.keys()
        return None if not prices else prices[0]

    def get_bids(self, price):
        return self._bids.get(price, {})


if __name__ == '__main__':
    import sys
    import time
    import datetime as dt
    from gdax import WebsocketClient


    class OrderBookConsole(OrderBook):
        ''' Logs real-time changes to the bid-ask spread to the console '''

        def __init__(self, product_id, public_client=None):
            super(OrderBookConsole, self).__init__(product_id=product_id, public_client=public_client)

            # latest values of bid-ask spread
            self._bid = None
            self._ask = None
            self._bid_depth = None
            self._ask_depth = None

        def on_orderbook_change(self):
            # Calculate newest bid-ask spread
            bid = self.get_bid()
            bids = self.get_bids(bid)
            bid_depth = sum([b['size'] for b in bids.values()])
            ask = self.get_ask()
            asks = self.get_asks(ask)
            ask_depth = sum([a['size'] for a in asks.values()])

            if self._bid == bid and self._ask == ask and self._bid_depth == bid_depth and self._ask_depth == ask_depth:
                # If there are no changes to the bid-ask spread since the last update, no need to print
                pass
            else:
                # If there are differences, update the cache
                self._bid = bid
                self._ask = ask
                self._bid_depth = bid_depth
                self._ask_depth = ask_depth
                print('{} {} bid: {:.3f} @ {:.2f}\task: {:.3f} @ {:.2f}'.format(
                    dt.datetime.now(), self.product_id, bid_depth, bid, ask_depth, ask))


    order_book = OrderBookConsole("BTC-EUR")

    websocket_client = WebsocketClient(listener=order_book)
    websocket_client.subscribe("full", ["BTC-EUR"])

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        websocket_client.close()

    if websocket_client.error:
        sys.exit(1)
    else:
        sys.exit(0)
