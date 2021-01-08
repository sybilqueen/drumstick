from google.cloud import bigquery

client = bigquery.Client()
table_id = "drumstick.uma.SynthInfo"

schema = [
    bigquery.SchemaField("address", "STRING"),
    bigquery.SchemaField("collateralToken", "STRING"),
    bigquery.SchemaField("syntheticToken", "STRING"),
    bigquery.SchemaField("totalPositionCollateral", "FLOAT64"), 
    bigquery.SchemaField("totalTokensOutstanding", "FLOAT64"), 
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("collateralTokenName", "STRING"),
    bigquery.SchemaField("syntheticTokenName", "STRING"),
]

table = bigquery.Table(table_id, schema=schema)
table.clustering_fields = ["timestamp", "address"]
table = client.create_table(table)


print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
