import pandas as pd
import random
import uuid

def gen_dup_data(num=100):
    rows = []
    for i in range(num):
        row = {} 

        row['twitter_name'] = 'Alice'
        row['twitter_token'] = round(random.random(), 1)
        row['uuid'] = str(uuid.uuid4())

        rows.append(row)



    df = pd.DataFrame(rows)

    return df

def write_to_csv(df):
    df.to_csv('twitter.csv.gz', compression='gzip', header=False, index=False)

if __name__ == '__main__':
    df = gen_dup_data(num=1000000)
    write_to_csv(df)


