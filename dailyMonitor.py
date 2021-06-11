#!/usr/bin/python3

import json
import os
import mysql.connector
import shutil
from datetime import datetime
import time

config = {
  'user': 'wright',
  'password': 'P@ssw0rd',
  'host': '193.123.240.188',
  'database': 'rchain_monitor',
  'raise_on_warnings': True
}

dataFolder="data/"

cnx = mysql.connector.connect(**config)

cursor = cnx.cursor()

def moveProcessedFile(filename):
    shutil.move(dataFolder + filename, dataFolder + "backup/" + filename)

def processAccountInfo(filename):
    with open(dataFolder + filename, 'r') as f:
        data = json.load(f)['data']
        saveData = ("INSERT INTO account_overview "
               "(total_accounts, 1d_active_account, 1w_active_account, 1m_active_account, top10, top50, top100) "
               "VALUES (%s, %s, %s, %s, %s, %s, %s)")
        accountInfo = (data['totalAccount'], data['last24hActiveAccountAmount'], data['last7dActiveAccountAmount'], data['last1mActiveAccountAmount'], data['top10']/100000000, data['top50']/100000000, data['top100']/100000000)
        cursor.execute(saveData, accountInfo)
    moveProcessedFile(filename)
    return

def processMxcInfo(filename):
    with open(dataFolder + filename, 'r') as f:
        data = json.load(f)['account']
        saveData = ("INSERT INTO daily_account_monitor "
               "(address, date, balance) "
               "VALUES (%s, %s, %s)")
        addressInfo = (data['address'], filename.split(".")[2], data['balance']/100000000)
        cursor.execute(saveData, addressInfo)
    moveProcessedFile(filename)
    return

def processMxcTransaction(filename):
    date = filename.split(".")[2]
    today = datetime.strptime(date, '%Y-%m-%d')
    with open(dataFolder + filename, 'r') as f:
        transactions = json.load(f)['transactions']
        saveData = ("INSERT INTO daily_account_transactions "
               "(blockNumber, fromAddr, toAddr, amount, timestamp) "
               "VALUES (%s, %s, %s, %s, %s)")
        for transaction in transactions:
            transactionData = datetime.utcfromtimestamp(transaction["timestamp"]/1000.0)
            if transaction["transactionType"] == 'transfer' and transactionData > today:
                transactionInfo = (transaction["blockNumber"], transaction["fromAddr"], transaction["toAddr"], transaction["amount"]/10000000, transaction["timestamp"])
                cursor.execute(saveData, transactionInfo)
    moveProcessedFile(filename)
    return

def processTop100(filename):
    date = filename.split(".")[2]
    with open(dataFolder + filename, 'r') as f:
        accounts = json.load(f)['accounts']
        saveData = ("INSERT INTO daily_top_accounts "
               "(date, account, balance) "
               "VALUES (%s, %s, %s)")
        for account in accounts:
            accountInfo = (date, account['address'], account['balance']/100000000)
            cursor.execute(saveData, accountInfo)
    moveProcessedFile(filename)
    return


for filename in os.listdir(dataFolder):
    if filename.startswith("accountInfo"): 
        processAccountInfo(filename)
    elif filename.startswith("mxcInfo"):
        processMxcInfo(filename)
    elif filename.startswith("mxcTransaction"):
        processMxcTransaction(filename)
    elif filename.startswith("top100"):
        processTop100(filename)

cnx.commit()
cursor.close()
cnx.close()
