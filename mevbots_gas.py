import os
import time,datetime
import pandas as pd
import etherscan_api

def conv_dt_rev(dt_int):
    """
    convert datetime format
    """
    return datetime.datetime(1970,1,1,0,0,0)+datetime.timedelta(seconds=int(dt_int)/1e0)

eth=etherscan_api.EtherscanConnector()
#eth=EtherscanConnector()

bot1 = '0xbeefbabeea323f07c59926295205d3b7a17e8638'
bot2 = '0x00000000c2cf7648c169b25ef1c217864bfa38cc'
bot3 = '0x0000000000007f150bd6f54c40a34d7c3d5e9f56'

def dailygas(address):
    txs = eth.get_normal_transactions(address=address)
    txs_pd = pd.DataFrame()

    for tx in txs:
        if tx['isError'] == '0':
            time_ = conv_dt_rev(tx['timeStamp'])
            txs_pd.loc[time_,'block'] = tx['blockNumber']
            txs_pd.loc[time_,'gasused'] = float(tx['gasUsed'])
            txs_pd.loc[time_,'gasprice'] = float(tx['gasPrice'])
            
    txs_pd['gasfee'] = txs_pd['gasused'] * txs_pd['gasprice'] / 1e18
    txs_pd['date'] = txs_pd.index
    txs_pd['date'] = txs_pd['date'].apply(lambda x: x.date())
    return txs_pd



bot1gas = dailygas(bot1)
bot2gas = dailygas(bot2)
bot3gas = dailygas(bot3)


print('This is a summary of how much each bot spends on gas')
print('------------------------------------------------------')
print('bot1 daily gas fees (in ETH): ',bot1gas.groupby('date')['gasfee'].sum())
print('bot1 daily txs amount: ', bot1gas.groupby('date')['gasfee'].count())
print('----------------------------------------------')
print('bot2 daily gas fees (in ETH): ',bot2gas.groupby('date')['gasfee'].sum())
print('bot2 daily txs amount: ', bot2gas.groupby('date')['gasfee'].count())
print('----------------------------------------------')
print('bot3 daily gas fees (in ETH): ',bot3gas.groupby('date')['gasfee'].sum())
print('bot3 daily txs amount: ', bot3gas.groupby('date')['gasfee'].count())