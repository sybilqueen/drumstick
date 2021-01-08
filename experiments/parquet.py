import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import random
import uuid

def gen_schema():
    parsed_schema = pa.schema([
        ('twitter_name', pa.string()),
        ('twitter_token', pa.float32()),
        ('uuid', pa.string())
    ])

    return parsed_schema

def gen_dup_data(num=100):
    rows = []
    for i in range(num):
        row = {} 

        row['twitter_name'] = 'Alice'
        row['twitter_token'] = round(random.random(), 1)
        row['uuid'] = str(uuid.uuid4())

        rows.append(row)

    return rows

def write_to_parquet(rows, parsed_schema):
    twitter_names = pa.array(
            list(map(lambda elt: elt['twitter_name'], rows)),
            type = pa.string()
    )

    twitter_tokens = pa.array(
            list(map(lambda elt: elt['twitter_token'], rows)),
            type = pa.float32()
    )

    uuids = pa.array(
            list(map(lambda elt: elt['uuid'], rows)),
            type = pa.string()
    )

    batch = pa.RecordBatch.from_arrays(
        [twitter_names, twitter_tokens, uuids],
        schema = parsed_schema
    )

    table = pa.Table.from_batches([batch])

    pq.write_table(table, 'twitter.parquet', compression='snappy')

if __name__ == '__main__':
    parsed_schema = gen_schema()
    df = gen_dup_data(num=1000000)
    write_to_parquet(df, parsed_schema)


