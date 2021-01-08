from google.cloud import bigquery

client = bigquery.Client()
table_id = "drumstick.uma.AMMPos"

schema = [
    bigquery.SchemaField("address", "STRING"), #address of Pool
    bigquery.SchemaField("label", "STRING"), #name
    bigquery.SchemaField("tokenA", "STRING"), #adr tokenA
    bigquery.SchemaField("tokenB", "STRING"), #adr tokenB
    bigquery.SchemaField("poolTokenAbalance", "float"),
    bigquery.SchemaField("poolTokenBbalance", "float"),
    bigquery.SchemaField("LPtokenBalance", "float"), #ourlp token balance
    bigquery.SchemaField("tokenAbalance", "float"), #our A tokens
    bigquery.SchemaField("tokenBbalance", "float"),
    bigquery.SchemaField("tokenAweight", "float"),
    bigquery.SchemaField("tokenBweight", "float"),
    bigquery.SchemaField("timestamp", "TIMESTAMP")
] #JOIN WITH ADDRESS TABLE TO GET INFO ABOUT AMM positions

table = bigquery.Table(table_id, schema=schema)
table.clustering_fields = ["timestamp", "address"]
table = client.create_table(table)


print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
