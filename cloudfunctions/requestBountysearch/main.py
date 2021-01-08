import os

from google import auth as google_auth
from google.auth.transport import grpc as google_auth_transport_grpc
from google.auth.transport import requests as google_auth_transport_requests
from google.cloud import bigquery
from google.cloud import storage

import requests
import redis

#internal imports
import bounty_searcher_pb2
import bounty_searcher_pb2_grpc

BUCKET_NAME = os.getenv('BUCKET_NAME')
DATASET_ID = os.getenv('DATASET_ID')
TABLE_NAME = os.getenv('TABLE_NAME')
DATA_PROJECT_NAME = os.getenv('DATA_PROJECT_NAME')
PROJECT_NAME = os.getenv('PROJECT_NAME')
REDISHOST = os.getenv('REDISHOST')
REDISPORT = os.getenv('REDISPORT')
GRPC_SERVICE = os.getenv('GRPC_SERVICE')
GRPC_SERVICE_URL = os.getenv('GRPC_SERVICE_URL')

MAX_SINCE_ID_KEY = f'{PROJECT_NAME}.bountysearch.max_since_id'

def requestBountysearch(_req):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <http://flask.pocoo.org/docs/1.0/api/#flask.Request>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>.
    """

    print(f'reading redis {REDISHOST}:{REDISPORT} for latest max_since_id')    
    redis_client = redis.StrictRedis(host=REDISHOST, port=REDISPORT)
    max_since_id = redis_client.get(MAX_SINCE_ID_KEY)

    if max_since_id:
        max_since_id = int(max_since_id)

    # call GRPC service
    print(f'querying grpc service at {GRPC_SERVICE} with since_id={max_since_id}...')

    rpc_resp = send_grpc(max_since_id, GRPC_SERVICE)
    print(rpc_resp)

    print(f'proccessed {rpc_resp.processed_count} tweets.')

    if rpc_resp.processed_count:
        
        print(f'updating max_since_id in redis to {rpc_resp.max_id}')
        
        redis_client.set(MAX_SINCE_ID_KEY, int(rpc_resp.max_id))

        # upload big query
        filename = f'tweets_{rpc_resp.max_id}.avro'

        upload_to_bigquery(filename)

        # delete blob
        delete_blob(filename)
    
    redis_client.close()

    return f'processed {rpc_resp.processed_count}'


def send_grpc(since_id, service_addr):

    # set up credentials

    credentials, _proj = google_auth.default()

    goog_req = google_auth_transport_requests.Request()

    channel = google_auth_transport_grpc.secure_authorized_channel(credentials, goog_req, service_addr)

    stub = bounty_searcher_pb2_grpc.BountySearcherStub(channel)

    # send tweet processing request

    if since_id is not None:
        since_id = int(since_id)

    tweet_request = bounty_searcher_pb2.BountySearcherRequest(since_id=since_id)

    # getting auth headers
    auth_headers = getAuthHeaders()

    res, _call = stub.GetHomeTimeline.with_call(tweet_request, metadata=auth_headers)

    return res

def getAuthHeaders():
    # Set up metadata server request
    # See https://cloud.google.com/compute/docs/instances/verifying-instance-identity#request_signature
    metadata_server_token_url = 'http://metadata/computeMetadata/v1/instance/service-accounts/default/identity?audience='

    token_request_url = metadata_server_token_url + GRPC_SERVICE_URL
    token_request_headers = {'Metadata-Flavor': 'Google'}

    # Fetch the token
    token_response = requests.get(token_request_url, headers=token_request_headers)
    jwt = token_response.content.decode("utf-8")

    return (('authorization', f'bearer {jwt}'),)


def upload_to_bigquery(filename):

    bq_client = bigquery.Client(project=DATA_PROJECT_NAME)
    table_ref = bq_client.dataset(DATASET_ID).table(TABLE_NAME)
    job_config = bigquery.LoadJobConfig(
        autodetect=False,
        use_avro_logical_types=True,
        clustering_fields='screen_name',
        time_partitioning = bigquery.table.TimePartitioning(field='date'))

    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.source_format = bigquery.SourceFormat.AVRO

    uri = f"gs://{BUCKET_NAME}/{filename}"

    load_job = bq_client.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )  # API request

    print(f"Starting big query job {load_job.job_id}")

    load_job.result()  # Waits for table load to complete.

    print("big query load finished.")


def delete_blob(filename):

    storage_client = storage.Client(project=DATA_PROJECT_NAME)
    
    bucket = storage_client.bucket(BUCKET_NAME)
    
    blob = bucket.blob(filename)

    blob.delete()
    
    print(f"blob {filename} deleted from bucket {BUCKET_NAME}")