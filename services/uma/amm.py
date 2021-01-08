import os
import json
from web3 import Web3, eth
import requests
import time
from google.cloud import storage, bigquery
from tempfile import mkstemp
from pandas import read_json
from flask import Flask


app = Flask(__name__)
storage_client= storage.Client()
client = bigquery.Client()

w3key = os.environ.get('WEB3KEY')
etherkey = os.environ.get("ESKEY")

w3 = Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/{}'.format(w3key))) 

@app.route("/upload", methods=['GET'])
def call():
   
    eoa_query = """ 
        SELECT filtered.address
        FROM 
            (SELECT address, max(timestamp) as new_timestamp
            FROM uma.Addresses
            WHERE type = 4
            GROUP BY address
            ) AS filtered
         INNER JOIN uma.Addresses as original
         ON original.timestamp = filtered.new_timestamp
         WHERE live = true
            
        
    """

    uni_query = """ 
        SELECT filtered.address
        FROM 
            (SELECT address, max(timestamp) as new_timestamp
            FROM uma.Addresses
            WHERE type = 5
            GROUP BY address
            ) AS filtered
         INNER JOIN uma.Addresses as original
         ON original.timestamp = filtered.new_timestamp
         WHERE live = true
            
        
    """

    bal_query = """ 
        SELECT filtered.address
        FROM 
            (SELECT address, max(timestamp) as new_timestamp
            FROM uma.Addresses
            WHERE type = 6
            GROUP BY address
            ) AS filtered
         INNER JOIN uma.Addresses as original
         ON original.timestamp = filtered.new_timestamp
         WHERE live = true
            
        
    """

    eoa_job = client.query(eoa_query).result()
    eoa_records = [str(r["address"]) for r in [dict(row) for row in eoa_job]]

    uni_job = client.query(uni_query).result()
    uni_records = [str(r["address"]) for r in [dict(row) for row in uni_job]]

    bal_job = client.query(bal_query).result()
    bal_records = [str(r["address"]) for r in [dict(row) for row in bal_job]]

    
    
    positions_list = []
    timestamp = time.time()

    for pool in uni_records:
        r = requests.get('https://api.etherscan.io/api?module=contract&action=getabi&address={}&apikey={}'.format(pool, etherkey)).json()
        if (r != ""):
            abi = json.loads(r["result"])
            pool_add = Web3.toChecksumAddress(pool)
            pool_contract = w3.eth.contract(abi=abi, address=pool_add)

            for eoa in eoa_records:
                eoa_add = Web3.toChecksumAddress(eoa)

                LPtokenBalance = pool_contract.functions.balanceOf(eoa_add).call()

                if LPtokenBalance != None and float(LPtokenBalance) != 0:

                    position_json = {"address": str(pool), "label": "Uniswap Pool"}

                    tokenA = str(pool_contract.functions.token0().call())
                    tokenB = str(pool_contract.functions.token1().call())
                    position_json["tokenA"] = tokenA
                    position_json["tokenB"] = tokenB

                    reserves = pool_contract.functions.getReserves().call()
                    poolTokenAbalance = float(reserves[0])
                    poolTokenBbalance = float(reserves[1])
                    position_json["poolTokenAbalance"] = poolTokenAbalance
                    position_json["poolTokenBbalance"] = poolTokenBbalance

                    LPtokenBalance = float(LPtokenBalance) 
                    position_json["LPtokenBalance"] = LPtokenBalance

                    totalSupply = float(pool_contract.functions.totalSupply().call()) 
                    ownership_ratio = LPtokenBalance / totalSupply

                    tokenAbalance = poolTokenAbalance * ownership_ratio
                    tokenBbalance = poolTokenBbalance * ownership_ratio
                    position_json["tokenAbalance"] = tokenAbalance
                    position_json["tokenBbalance"] = tokenBbalance

                    position_json["tokenAweight"] = float(0.5)
                    position_json["tokenBweight"] = float(0.5)

                    position_json["timestamp"] = timestamp

                    positions_list.append(position_json)

    for pool in bal_records:
        r = requests.get('https://api.etherscan.io/api?module=contract&action=getabi&address={}&apikey={}'.format(pool, etherkey)).json()
        if (r != ""):
            abi = json.loads(r["result"])
            pool_add = Web3.toChecksumAddress(pool)
            pool_contract = w3.eth.contract(abi=abi, address=pool_add)

            for eoa in eoa_records:
                eoa_add = Web3.toChecksumAddress(eoa)

                LPtokenBalance = pool_contract.functions.balanceOf(eoa_add).call()

                if LPtokenBalance != None and float(LPtokenBalance) != 0:

                    position_json = {"address": str(pool), "label": "Balancer Pool"}

                    tokens = pool_contract.functions.getCurrentTokens().call()

                    tokenA = str(tokens[0])
                    tokenB = str(tokens[1])
                    position_json["tokenA"] = tokenA
                    position_json["tokenB"] = tokenB

                    aBalance = pool_contract.functions.getBalance(tokenA).call()
                    poolTokenAbalance = float(aBalance)
                    bBalance = pool_contract.functions.getBalance(tokenB).call()
                    poolTokenBbalance = float(bBalance)
                    position_json["poolTokenAbalance"] = poolTokenAbalance
                    position_json["poolTokenBbalance"] = poolTokenBbalance

                    LPtokenBalance = float(LPtokenBalance) 
                    position_json["LPtokenBalance"] = LPtokenBalance

                    totalSupply = float(pool_contract.functions.totalSupply().call()) 
                    ownership_ratio = LPtokenBalance / totalSupply

                    tokenAbalance = poolTokenAbalance * ownership_ratio
                    tokenBbalance = poolTokenBbalance * ownership_ratio
                    position_json["tokenAbalance"] = tokenAbalance
                    position_json["tokenBbalance"] = tokenBbalance

                    aWeight = pool_contract.functions.getNormalizedWeight(tokenA).call()
                    tokenAweight = float(aWeight) / (10**18)
                    bWeight = pool_contract.functions.getNormalizedWeight(tokenB).call()
                    tokenBweight = float(bWeight) / (10**18)
                    position_json["tokenAweight"] = tokenAweight 
                    position_json["tokenBweight"] = tokenBweight

                    position_json["timestamp"] = timestamp

                    positions_list.append(position_json)

        temp_json = json.dumps(positions_list)
        df = read_json(temp_json)
        df.fillna(0, inplace=True)
        df['address'] = df['address'].astype(str)
        df['tokenA'] = df['tokenA'].astype(str)
        df['tokenB'] = df['tokenB'].astype(str)

        fd, path = mkstemp()
        df.to_csv(path, index=False)

        bucket_name = "amm-positions"
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob('ammpos.csv')
        blob.upload_from_filename(path)

port = int(os.environ.get('PORT', 8080))

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=port)
