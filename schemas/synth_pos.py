from google.cloud import bigquery

client = bigquery.Client()
table_id = "drumstick.uma.SynthPos"

schema = [
    bigquery.SchemaField("address", "STRING"), #address of EMP
    bigquery.SchemaField("sponsor", "STRING"), #address of EOA 
    bigquery.SchemaField("PositionCollateral", "FLOAT64"), 
    bigquery.SchemaField("TokensOutstanding", "FLOAT64"), 
    bigquery.SchemaField("timestamp", "TIMESTAMP")
] #JOIN WITH ADDRESS TABLE TO GET INFO ABOUT TOKEN ADDRESSES/NAMES

table = bigquery.Table(table_id, schema=schema)
table.clustering_fields = ["timestamp", "address", "sponsor"]
table = client.create_table(table)


print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
