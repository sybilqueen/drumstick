from fastavro import writer, reader, parse_schema
import pandas as pd
import random
import uuid

def gen_schema():
    schema = {
        'doc': 'Twitter',
        'name': 'Twitter',
        'namespace': 'twitter',
        'type': 'record',
        'fields': [
            {'name': 'twitter_name', 'type': 'string'},
            {'name': 'twitter_token', 'type': 'float'},
            {'name': 'uuid', 'type': 'string'},
            ],
        }
    parsed_schema = parse_schema(schema)

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

def write_to_avro(records, parsed_schema):
    with open('twitter.avro', 'wb') as out:
        writer(out, parsed_schema, records, codec='snappy')

if __name__ == '__main__':
    parsed_schema = gen_schema()
    records = gen_dup_data(num=1000000)
    write_to_avro(records, parsed_schema)


