#!/usr/bin/python3

import json
import os
import mysql.connector
import shutil
from datetime import datetime
import requests

config = {
  'user': '',
  'password': '',
  'host': '',
  'database': '',
  'raise_on_warnings': True
}


today=datetime.today()
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()

def processAccountInfo():
    resp = requests.get('https://revdefine.io/define/api/stat/accounts')
    data = resp.json()['data']
    saveData = ("INSERT INTO account_overview "
           "(total_accounts, 1d_active_account, 1w_active_account, 1m_active_account, top10, top50, top100) "
           "VALUES (%s, %s, %s, %s, %s, %s, %s)")
    accountInfo = (data['totalAccount'], data['last24hActiveAccountAmount'], data['last7dActiveAccountAmount'], data['last1mActiveAccountAmount'], data['top10']/100000000, data['top50']/100000000, data['top100']/100000000)
    cursor.execute(saveData, accountInfo)
    return

def processMxcInfo():
    resp = requests.get('https://revdefine.io/define/api/revaccount/1111fTFCBE727Ex5AHDhAD38HyNca66U5vKVCoQDLwauVCY9DDbBX')
    data = resp.json()['account']
    saveData = ("INSERT INTO daily_account_monitor "
           "(address, date, balance) "
           "VALUES (%s, %s, %s)")
    addressInfo = (data['address'], today.strftime('%Y-%m-%d'), data['balance']/100000000)
    cursor.execute(saveData, addressInfo)
    return

def processMxcTransaction():
    page=0
    nextPage=True
    while nextPage:
        page += 1
        resp = requests.get('https://revdefine.io/define/api/transactions/1111fTFCBE727Ex5AHDhAD38HyNca66U5vKVCoQDLwauVCY9DDbBX?rowsPerPage=50&page=' + str(page))
        transactions = resp.json()['transactions']
        saveData = ("INSERT INTO daily_account_transactions "
               "(blockNumber, fromAddr, toAddr, amount, timestamp) "
               "VALUES (%s, %s, %s, %s, %s)")
        for transaction in transactions:
            transactionData = datetime.utcfromtimestamp(transaction["timestamp"]/1000.0)
            if transaction["transactionType"] == 'transfer' and transactionData > today:
                transactionInfo = (transaction["blockNumber"], transaction["fromAddr"], transaction["toAddr"], transaction["amount"]/10000000, transaction["timestamp"])
                cursor.execute(saveData, transactionInfo)
            elif transactionData < today:
                nextPage=False
    return

def processTop100():
    resp = requests.get('https://revdefine.io/define/api/revaccounts?rowsPerPage=100&page=1')
    accounts = resp.json()['accounts']
    saveData = ("INSERT INTO daily_top_accounts "
           "(date, account, balance) "
           "VALUES (%s, %s, %s)")
    for account in accounts:
        accountInfo = (today.strftime('%Y-%m-%d'), account['address'], account['balance']/100000000)
        cursor.execute(saveData, accountInfo)
    return



processAccountInfo()
processMxcInfo()
processMxcTransaction()
processTop100()

cnx.commit()
cursor.close()
cnx.close()
