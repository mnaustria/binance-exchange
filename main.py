#!/usr/local/bin/python3

import time
import requests
import pandas as pd
from prometheus_client import start_http_server, Gauge


class Client:

    API_URL = 'https://api.binance.com'

    def __init__(self):
        self.API_URL = self.API_URL
        self.prom_gauge = Gauge('absolute_delta_value',
                                'Absolute Delta Value of Price Spread', ['symbol'])

    def get_symbols(self, asset, field):
        uri = "/api/v3/ticker/24hr"

        response = requests.get(self.API_URL + uri)
        df = pd.DataFrame(response.json())
        df = df[['symbol', field]]
        df = df[df.symbol.str.contains(r'(?!$){}$'.format(asset))]
        df[field] = pd.to_numeric(df[field], downcast='float', errors='coerce')
        df = df.sort_values(by=[field], ascending=False).head(5)
        # print(df)

        return df

    def get_notional_value(self, asset, field):
        uri = "/api/v3/depth"

        symbols = self.get_symbols(asset, field)
        notional_list = {}

        for symbol in symbols['symbol']:
            payload = {'symbol': symbol, 'limit': 500}
            response = requests.get(self.API_URL + uri, params=payload)
            for col in ["bids", "asks"]:
                df = pd.DataFrame(data=response.json()[col], columns=[
                                  "price", "quantity"], dtype=float)
                df = df.sort_values(by=['price'], ascending=False).head(200)
                df['notional'] = df['price'] * df['quantity']
                df['notional'].sum()
                notional_list[symbol + '_' + col] = df['notional'].sum()

        return notional_list

    def get_price_spread(self, asset, field):
        uri = '/api/v3/ticker/bookTicker'

        symbols = self.get_symbols(asset, field)
        spread_list = {}

        for symbol in symbols['symbol']:
            payload = {'symbol': symbol}
            response = requests.get(self.API_URL + uri, params=payload)
            price_spread = response.json()
            spread_list[symbol] = float(
                price_spread['askPrice']) - float(price_spread['bidPrice'])

        return spread_list

    def get_spread_delta(self, asset, field):

        delta = {}
        old_spread = self.get_price_spread(asset, field)
        time.sleep(10)
        new_spread = self.get_price_spread(asset, field)

        for key in old_spread:
            delta[key] = abs(old_spread[key]-new_spread[key])

        for key in delta:
            self.prom_gauge.labels(key).set(delta[key])

        print("\n Absolute Delta for %s" % asset)
        print(delta)


if __name__ == "__main__":
    client = Client()

    print("\n1. Print the top 5 symbols with quote asset BTC and the highest volume over the last 24 hours in descending order.")
    print(client.get_symbols('BTC', 'volume'))

    print("\n2.  Print the top 5 symbols with quote asset USDT and the highest number of trades over the last 24 hours in descending order.")
    print(client.get_symbols('USDT', 'count'))

    print("\n3. Using the symbols from Q1, what is the total notional value of the top 200 bids and asks currently on each order book?")
    print(client.get_notional_value('BTC', 'volume'))

    print("\n4. What is the price spread for each of the symbols from Q2")
    print(client.get_price_spread('USDT', 'count'))

    print("\n5. Every 10 seconds print the result of Q4 and the absolute delta from the previous value for each symbol.")

    print("\n6. Make the output of Q5 accessible by querying http://localhost:8080/metrics using the Prometheus Metrics format.")
    start_http_server(8080)

    while True:
        client.get_spread_delta('USDT', 'count')
