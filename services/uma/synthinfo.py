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
   
    emp_query = """ 
        SELECT filtered.address
        FROM 
            (SELECT address, max(timestamp) as new_timestamp
            FROM uma.Addresses
            WHERE type = 3
            GROUP BY address
            ) AS filtered
         INNER JOIN uma.Addresses as original
         ON original.timestamp = filtered.new_timestamp
         WHERE live = true
        
    """

    eoa_query = """ 
       SELECT filtered.address
        FROM 
            (SELECT address, max(timestamp) as new_timestamp
            FROM uma.Addresses
            WHERE type = 2
            GROUP BY address
            ) AS filtered
         INNER JOIN uma.Addresses as original
         ON original.timestamp = filtered.new_timestamp
         WHERE live = true
            
        
    """

    emp_job = client.query(emp_query).result()
    eoa_job = client.query(eoa_query).result()
    emp_records = [str(r["address"]) for r in [dict(row) for row in emp_job]]
    eoa_records = [str(r["address"]) for r in [dict(row) for row in eoa_job]]

    
    positions_list = []
    timestamp = time.time()
    for record in emp_records:
        r = requests.get('https://api.etherscan.io/api?module=contract&action=getabi&address={}&apikey={}'.format(record, etherkey)).json()
        if (r != ""):
            abi = json.loads(r["result"])
            emp_address = Web3.toChecksumAddress(record)
            emp_contract = w3.eth.contract(abi=abi, address=emp_address)
            for eoa in eoa_records:
                eoa_add = Web3.toChecksumAddress(eoa)
                collateral = emp_contract.functions.getCollateral(eoa_add).call()
                if collateral != None and float(collateral[0]) != 0:
                    outstanding = emp_contract.functions.positions(eoa_add).call()
                    outstanding = float(outstanding[0][0])
                    position_json = {"address" : str(record), "sponsor" : str(eoa), "PositionCollateral" : float(collateral[0]),
                     "TokensOutstanding" : float(outstanding), "timestamp" : timestamp}
                    positions_list.append(position_json)
    
    temp_json = json.dumps(positions_list)
    df = read_json(temp_json)
    df.fillna(0, inplace=True)
    df['address'] = df['address'].astype(str)
    df['sponsor'] = df['sponsor'].astype(str)

    fd, path = mkstemp()
    df.to_csv(path, index=False)

    bucket_name = "synth-positions"
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob('synthpos.csv')
    blob.upload_from_filename(path)


port = int(os.environ.get('PORT', 8080))

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=port)
