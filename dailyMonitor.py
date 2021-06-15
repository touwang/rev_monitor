#!/usr/bin/python3

import json
import os
import mysql.connector
import shutil
from datetime import datetime
from datetime import date
from datetime import timedelta
import requests

config = {
  'user': '',
  'password': '',
  'host': '',
  'database': '',
  'raise_on_warnings': True
}

today = date.today()
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor(buffered=True)


def process_accountinfo():
    resp = requests.get('https://revdefine.io/define/api/stat/accounts')
    data = resp.json()['data']
    save_data = ("INSERT INTO account_overview "
                 "(total_accounts, 1d_active_account, 1w_active_account, 1m_active_account, top10, top50, top100) "
                 "VALUES (%s, %s, %s, %s, %s, %s, %s)")
    account_info = (data['totalAccount'], data['last24hActiveAccountAmount'], data['last7dActiveAccountAmount'],
                    data['last1mActiveAccountAmount'], data['top10'] / 100000000, data['top50'] / 100000000,
                    data['top100'] / 100000000)
    try:
        cursor.execute(save_data, account_info)
        print("Save daily account info (" + str(datetime.today()) + "):")
        print(repr(account_info))
    except mysql.connector.Error as err:
        print("Failed to save Daily Account Info: {}".format(err))
    return


def process_mxcInfo():
    resp = requests.get(
        'https://revdefine.io/define/api/revaccount/1111fTFCBE727Ex5AHDhAD38HyNca66U5vKVCoQDLwauVCY9DDbBX')
    data = resp.json()['account']
    save_data = ("INSERT INTO daily_account_monitor "
                 "(address, date, balance) "
                 "VALUES (%s, %s, %s)")
    address_info = (data['address'], today, data['balance'] / 100000000)
    try:
        cursor.execute(save_data, address_info)
        print("Save MXC info (" + str(today) + "):")
        print(repr(address_info))
    except mysql.connector.Error as err:
        print("Failed to save MXC Info: {}".format(err))
    return


def process_mxctransaction():
    page = 0
    transaction_count = 0
    next_page = True
    yesterday = today - timedelta(days=1)
    last_transaction_datetime = datetime.combine(yesterday, datetime.min.time())
    query = "SELECT timestamp FROM daily_account_transactions order by timestamp desc limit 1"
    cursor.execute(query)
    if cursor.rowcount == 1:
        for (transaction_datetime,) in cursor:
            last_transaction_datetime = transaction_datetime
    while next_page:
        page += 1
        resp = requests.get(
            'https://revdefine.io/define/api/transactions/1111fTFCBE727Ex5AHDhAD38HyNca66U5vKVCoQDLwauVCY9DDbBX?rowsPerPage=50&page=' + str(page))
        # print('https://revdefine.io/define/api/transactions/1111fTFCBE727Ex5AHDhAD38HyNca66U5vKVCoQDLwauVCY9DDbBX?rowsPerPage=50&page=' + str(page))
        transactions = resp.json()['transactions']
        # print(repr(transactions))
        save_data = ("INSERT INTO daily_account_transactions "
                     "(blockNumber, fromAddr, toAddr, amount, timestamp) "
                     "VALUES (%s, %s, %s, %s, %s)")
        try:
            for transaction in transactions:
                transaction_date = datetime.utcfromtimestamp(int(transaction["timestamp"] / 1000.0))
                # print(repr(transaction))
                if transaction["transactionType"] == 'transfer' and transaction_date > last_transaction_datetime:
                    transaction_info = (transaction["blockNumber"], transaction["fromAddr"], transaction["toAddr"],
                                        transaction["amount"] / 10000000, transaction_date)
                    cursor.execute(save_data, transaction_info)
                    transaction_count += 1
                elif transaction["transactionType"] == 'transfer' and transaction_date <= last_transaction_datetime:
                    next_page = False
                    break
        except mysql.connector.Error as err:
            print("Failed to save MXC Transaction Info: {}".format(err))
        print("Save MXC Transaction info (" + str(today) + "):" + str(transaction_count))
    return


def process_top100():
    resp = requests.get('https://revdefine.io/define/api/revaccounts?rowsPerPage=100&page=1')
    accounts = resp.json()['accounts']
    save_data = ("INSERT INTO daily_top_accounts "
                 "(date, account, balance) "
                 "VALUES (%s, %s, %s)")
    try:
        for account in accounts:
            account_info = (today, account['address'], account['balance'] / 100000000)
            cursor.execute(save_data, account_info)
        print("Save Top100 Account info (" + str(today) + ")")
    except mysql.connector.Error as err:
        print("Failed to save Top100 Account Info: {}".format(err))
    return


def getdailychange():
    yesterday = today - timedelta(days=1)
    mxc_address = "1111fTFCBE727Ex5AHDhAD38HyNca66U5vKVCoQDLwauVCY9DDbBX"
    # get total accounts changes
    accountinfo_query = (
        "SELECT total_accounts, 1d_active_account, 1w_active_account, 1m_active_account, top10, top50, top100 FROM account_overview "
        "order by date desc limit 2")
    cursor.execute(accountinfo_query)
    data = []
    for (total_accounts, d_active_account, w_active_account, m_active_account, top10, top50, top100) in cursor:
        row = {"total_accounts": total_accounts, "d_active_account": d_active_account,
               "w_active_account": w_active_account, "m_active_account": m_active_account, "top10": top10,
               "top50": top50, "top100": top100}
        data.append(row)
    total_account_changes = """
Total Accounts Changes: {}
1 Day Active Accounts: {}
1 Week Active Accounts: {}
1 Month Active Accounts: {}
Top 10: {}
Top 50: {}
Top 100: {}
    """
    print(total_account_changes.format((data[0]["total_accounts"] - data[1]["total_accounts"]),
                                       (data[0]["d_active_account"] - data[1]["d_active_account"]),
                                       (data[0]["w_active_account"] - data[1]["w_active_account"]),
                                       (data[0]["m_active_account"] - data[1]["m_active_account"]),
                                       (data[0]["top10"] - data[1]["top10"]),
                                       (data[0]["top50"] - data[1]["top50"]),
                                       (data[0]["top100"] - data[1]["top100"])))
    # get daily top accounts changes
    before = today - timedelta(days=2)
    topaccount_query = ("SELECT date, account, balance FROM daily_top_accounts "
             "where date > '{}'".format(before.strftime('%Y-%m-%d')))
    # parameter should be a tuple
    # queryParam = (before.strftime('%Y-%m-%d'), )
    cursor.execute(topaccount_query)
    top_accounts = {}
    for (date, account, balance) in cursor:
        if not date.strftime('%Y-%m-%d') in top_accounts:
            top_accounts[date.strftime('%Y-%m-%d')] = {}
        top_accounts[date.strftime('%Y-%m-%d')][account] = balance
    # print(repr(top_accounts))
    yesterdaystr = str(yesterday)
    top100_changes = "Top 100 Accounts:\n"
    for account, balance in top_accounts[str(today)].items():
        if account in top_accounts[yesterdaystr] and (balance - top_accounts[yesterdaystr][account] != 0):
            top100_changes += account + ":" + str(balance - top_accounts[yesterdaystr][account]) + "\n"
        elif not account in top_accounts[yesterdaystr]:
            top100_changes += account + ":" + str(balance) + "(new)\n"

    print(top100_changes)
    # get MXC(1111fTFCBE727Ex5AHDhAD38HyNca66U5vKVCoQDLwauVCY9DDbBX) changes
    mxcinfo_query = (
        "SELECT balance FROM daily_account_monitor where address=%s "
        "order by date desc limit 2")
    cursor.execute(mxcinfo_query, (mxc_address,))
    data = []
    for (balance,) in cursor:
        data.append(balance)
    mxc_changes = "MXC balance Change:" + str(int(data[0] - data[1]))
    print(mxc_changes)

    # get MXC(1111fTFCBE727Ex5AHDhAD38HyNca66U5vKVCoQDLwauVCY9DDbBX) transactions
    yesterday_datetime = datetime.combine(yesterday, datetime.min.time())
    mxctransaction_query = (
        "select count(*), sum(amount) from daily_account_transactions where fromAddr=%s and timestamp > %s")
    cursor.execute(mxctransaction_query, (mxc_address, yesterday_datetime))
    for (count, amount) in cursor:
        print("total out transactions: " + str(count) + ", sum: " + str(int(amount)))
    mxctransaction_query = (
        "select count(*), sum(amount) from daily_account_transactions where toAddr=%s and timestamp > %s")
    cursor.execute(mxctransaction_query, (mxc_address, yesterday_datetime))
    for (count, amount) in cursor:
        print("total in transactions: " + str(count) + ", sum: " + str(int(amount)))

process_accountinfo()
process_mxcInfo()
process_mxctransaction()
process_top100()

cnx.commit()

getdailychange()

cursor.close()
cnx.close()
