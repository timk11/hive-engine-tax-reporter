#!/usr/bin/env python
# coding: utf-8

# # Hive-Engine Tax Reporter
# This script will produce a CSV file compatible with Koinly, which can be modified for other tax report formats.
# The resulting file may possibly contain errors or duplications, so please check the file manually before using it.


from hiveengine.api import Api
from hiveengine.wallet import Wallet
from beem import Hive
from datetime import datetime as dt
import pandas as pd
import requests
from io import BytesIO, StringIO
from flask import Flask, request, make_response

app = Flask(__name__)

@app.route('/')
def index():
    return (
        """<form action="get_csv" method="get">
                Hive-Engine Tax Reporter<br><br>Account name: <input type="text" name="account_name">
                <input type="submit" value="Transaction report">
              </form>"""
    )

@app.route('/get_csv')
def get_csv():
    account_name = request.args.get("account_name", "")
    if account_name:
        transaction_report = tx_report(account_name)
        sio = StringIO()
        transaction_report.to_csv(sio, index=False)
        output = make_response(sio.getvalue())
        output.headers["Content-Disposition"] = "attachment; filename=" + filename
        output.headers["Content-type"] = "text/csv"
    else:
        output = ""
    return output


def tx_report(account_name):
    api = Api()

    balances = Wallet(account_name, blockchain_instance=Hive()).get_balances()
    tokens = [balance['symbol'] for balance in balances]

    df = pd.DataFrame()
    for token in tokens:
        for line in api.get_history(account_name, token):
            df = df.append(line, ignore_index=True)

    dates = []
    for ix in df.index:
        date = dt.fromtimestamp(df['timestamp'][ix])
        dates.append(dt.strptime(str(date), '%Y-%m-%d %H:%M:%S'))
    df['DateTime'] = dates

    columns = ['Date', 'Sent Amount', 'Sent Currency', 'Received Amount', 'Received Currency',
               'Description', 'Net Worth Amount', 'Net Worth Currency', 'TxHash']
    report = pd.DataFrame(columns=columns)
    for ix in df.index:
        if df['operation'][ix] == 'market_buy':
            report = report.append({'Date': df['DateTime'][ix], 'Sent Amount': df['quantityHive'][ix], 'Sent Currency': 'SWAP.HIVE',
                            'Received Amount': df['quantityTokens'][ix], 'Received Currency': df['symbol'][ix],
                            'Description': df['operation'][ix], 'TxHash': df['transactionId'][ix]}, ignore_index=True)
        elif df['operation'][ix] == 'market_sell':
            report = report.append({'Date': df['DateTime'][ix], 'Sent Amount': df['quantityTokens'][ix],
                            'Sent Currency': df['symbol'][ix], 'Received Amount': df['quantityHive'][ix], 'Received Currency': 'SWAP.HIVE',
                            'Description': df['operation'][ix], 'TxHash': df['transactionId'][ix]}, ignore_index=True)
        elif df['quantity'].isnull()[ix]:
            continue
        elif df['from'][ix] == account_name:
            report = report.append({'Date': df['DateTime'][ix], 'Sent Amount': df['quantity'][ix], 'Sent Currency': df['symbol'][ix],
                            'Description': df['operation'][ix], 'TxHash': df['transactionId'][ix]}, ignore_index=True)
        elif df['to'][ix] == account_name:
            report = report.append({'Date': df['DateTime'][ix], 'Received Amount': df['quantity'][ix], 'Received Currency': df['symbol'][ix],
                            'Description': df['operation'][ix], 'TxHash': df['transactionId'][ix]}, ignore_index=True)
        else:
            print("This transaction was not included:\n", df.loc[ix].to_dict(),"\n")

    report['Net Worth Currency'] = 'USD'

    response = requests.get('https://api.coingecko.com/api/v3/coins/hive/market_chart?vs_currency=usd&days=max')
    response.raise_for_status()
    hive_price = pd.DataFrame(response.json()['prices'], columns=['timestamp', 'price'])
    hive_price.timestamp = hive_price.timestamp/1000
    hive_price['date'] = pd.to_datetime(hive_price.timestamp, unit="s")

    sent_df = report.dropna(subset=['Sent Currency'])
    rec_df = report.dropna(subset=['Received Currency'])
    for token in tokens:
        price_df = price_history(token)
        if len(price_df) == 0:
            continue
        for ix in sent_df[sent_df['Sent Currency'].str.contains(token)].index:
            idx = (price_df.timestamp < dt.strptime(str(sent_df.loc[ix].Date), '%Y-%m-%d %H:%M:%S').timestamp()).idxmax() - 1
            if idx < 0:
                idx = 0
            ixh = (hive_price.timestamp >= dt.strptime(str(sent_df.loc[ix].Date), '%Y-%m-%d %H:%M:%S').timestamp()).idxmax()
            try:
                report.loc[ix, 'Net Worth Amount'] = float(price_df['openPrice'][idx]) * float(report['Sent Amount'][ix]) * hive_price['price'][ixh]
            except:
                print("This transaction was not included:\n", report.loc[ix].to_dict(),"\n")
        for ix in rec_df[rec_df['Received Currency'].str.contains(token)].index:
            idx = (price_df.timestamp < dt.strptime(str(rec_df.loc[ix].Date), '%Y-%m-%d %H:%M:%S').timestamp()).idxmax() - 1
            if idx < 0:
                idx = 0
            ixh = (hive_price.timestamp >= dt.strptime(str(rec_df.loc[ix].Date), '%Y-%m-%d %H:%M:%S').timestamp()).idxmax()
            try:
                report.loc[ix, 'Net Worth Amount'] = float(price_df['openPrice'][idx]) * float(report['Received Amount'][ix]) * hive_price['price'][ixh]
            except:
                print("This transaction was not included:\n", report.loc[ix].to_dict(),"\n")

    token = 'SWAP.HIVE'
    for ix in sent_df[sent_df['Sent Currency'].str.contains(token)].index:
        ixh = (hive_price.timestamp >= dt.strptime(str(sent_df.loc[ix].Date), '%Y-%m-%d %H:%M:%S').timestamp()).idxmax()
        try:
            report.loc[ix, 'Net Worth Amount'] = float(report['Sent Amount'][ix]) * hive_price['price'][ixh]
        except:
            print("This transaction was not included:\n", report.loc[ix].to_dict(),"\n")
    for ix in rec_df[rec_df['Received Currency'].str.contains(token)].index:
        ixh = (hive_price.timestamp >= dt.strptime(str(rec_df.loc[ix].Date), '%Y-%m-%d %H:%M:%S').timestamp()).idxmax()
        try:
            report.loc[ix, 'Net Worth Amount'] = float(report['Received Amount'][ix]) * hive_price['price'][ixh]
        except:
            print("This transaction was not included:\n", report.loc[ix].to_dict(),"\n")

    for ix in sent_df.index:
        if report.loc[ix, 'Sent Currency'][:5] == 'SWAP.':
            report.loc[ix, 'Sent Currency'] = report.loc[ix, 'Sent Currency'][5:]
    for ix in rec_df.index:
        if report.loc[ix, 'Received Currency'][:5] == 'SWAP.':
            report.loc[ix, 'Received Currency'] = report.loc[ix, 'Received Currency'][5:]

    global filename
    filename = "Hive-Engine_txs_" + account_name + "_" + dt.strftime(dt.now(), "%Y%m%d_%H%M%S") + ".csv"

    return report


def price_history(token):
    response = requests.get('https://accounts.hive-engine.com/marketHistory', params=dict(symbol=token))
    response.raise_for_status()
    price_data = pd.json_normalize(response.json())
    try:
        price_data['date'] = pd.to_datetime(price_data.timestamp, unit="s")
    except:
        pass
    return price_data


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
