import asyncio
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from io import BytesIO
import json
import logging
import logging.handlers
import os
import re

# non built-in imports
from fastavro import writer, parse_schema
from google.cloud import storage
from grpc.experimental import aio
import spacy
import tweepy
import bounty_searcher_pb2
import bounty_searcher_pb2_grpc

#----------------------------------------------------------------
# Constants

APPLICATION_BASE_NAME = "BountySearcher"
LOG_FORMATTER = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
EPOCH = datetime(1970, 1, 1)

TWEET_AVRO_SCHEMA = {
    'name': 'tweet',
    'type': 'record',
    'fields': [
        {'name': 'screen_name', 'type': 'string'},
        {'name': 'display_name', 'type': 'string'},
        {'name': 'user_id', 'type': 'string'},
        {'name': 'is_retweet', 'type': 'boolean'},
        {'name': 'text', 'type': 'string'},
        {'name': 'id', 'type': 'long'},
        {'name': 'contains_media', 'type': 'boolean'},
        {'name': 'client_type', 'type': 'string'},
        {'name': 'time', 'type': {'type': 'long', 'logicalType' : 'timestamp-millis'}},
        {'name': 'date', 'type': {'type':'int', 'logicalType' : 'date'}},
        {'name': 'token_count', 'type': {'type': 'map', 'values':'long'}, 'default':{}},
        {'name': 'ner_count', 'type': {'type': 'map', 'values':'long'}, 'default':{}},
        {'name': 'regex_count', 'type': {'type': 'map', 'values':'long'}, 'default':{}},
    ],
}

PARSED_SCHEMA = parse_schema(TWEET_AVRO_SCHEMA)

#----------------------------------------------------------------
# Service Parameters

# maximum number of home timeline requests supported by twitter API
MAX_HOME_TIMELINE_TWEETS_PER_REQUEST = int(os.getenv('MAX_HOME_TIMELINE_TWEETS_PER_REQUEST', '200'))

# Used to compute the max total tweets we try to request at once
WINDOW_SIZE = int(os.getenv('WINDOW_SIZE', '10'))

# upper bound tweets this service will try to retrieve at once to avoid exceeding rate limit window
MAX_TWEETS_PER_WINDOW = MAX_HOME_TIMELINE_TWEETS_PER_REQUEST * WINDOW_SIZE

# port that this service will run on
SERVICE_PORT = int(os.getenv('PORT', '50011'))

# for a given request, we will wait this many seconds for tweepy to retrieve results
TWEEPY_TIMEOUT = int(os.getenv('TWEEPY_TIMEOUT', '60'))

# Spacey Model to load
NLP_MODEL = os.getenv('NLP_MODEL') 

# regex to match
REGEX_PATTERN = os.getenv('REGEX_PATTERN')

# bucket name to store to in google cloud storage
BUCKET_NAME = os.getenv('BUCKET_NAME')


class BountySearcherService(bounty_searcher_pb2_grpc.BountySearcherServicer):
    """Provides methods that implement functionality of tweet manager servicer"""

#------------------------------------------------------------------------------
# Control Plane

    def __init__(self, credentials):
        """initializer reads in authorization keys and secrets from environment
        variables by default"""

        # authorizing twitter service
        auth = tweepy.OAuthHandler(credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
        auth.set_access_token(credentials['ACCESS_KEY'],credentials['ACCESS_SECRET'])
        
        self._api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

        logger.info('twitter authorization authenticated')

        # we only want one thread running since we only have 1 set of credentials. 
        # Should look for an aio based solution down the line
        self._tweepyExecutor = ThreadPoolExecutor(max_workers=1)

        # loading model 
        self._nlp = spacy.load(NLP_MODEL, disable=["tagger","parser"])
        logger.info('NLP model %s loaded', NLP_MODEL)

        # google cloud storage client
        self._storage_client = storage.Client()

#-----------------------------------------------------------------
# Data Plane

    #----------------------------------------------------------------
    # gRPC Service Implementations

    async def GetHealth(self, request, _context):
        """Gets health"""
        target = bounty_searcher_pb2.HealthResponse()
        target.health = 42

        return target

    async def GetHomeTimeline(self, request, _context):
        """implementation of GetHomeTimeline RPC call"""
        # default to None to avoid inadvertently using 0 as id value

        since_id = None if not request.since_id else request.since_id
        max_id = None if not request.max_id else request.max_id
        
        logger.info("received gRPC request with since_id %s and max_id %s", since_id, max_id)
        payload = await self._load(since_id, max_id)
        logger.debug("gRPC request complete")

        return payload

    #----------------------------------------------------------------

    def _extract(self, since_id, max_id):
        """extracts tweet status objects via tweepy cursor, and returns iterator
        """
        return tweepy.Cursor(
            self._api.home_timeline,
            tweet_mode='extended', # we want the full tweet text regardless of 140 char limit
            include_rts=True,
            count=MAX_HOME_TIMELINE_TWEETS_PER_REQUEST,
            since_id=since_id,
            max_id=max_id).items(MAX_TWEETS_PER_WINDOW)

    def _tweet_statuses_loader(self, since_id, max_id):
        """loads transformed payload into list of target tweets. Computes the max since id in the list of tweets"""
        tweets = []
        max_since_id = 0
        for transformed in transform(self._extract(since_id, max_id)):
            # update max since id we saw up until now
            max_since_id = max(max_since_id, transformed['id'])

            # append transformed
            tweets.append(transformed)

        return tweets, max_since_id

    async def _load(self, since_id, max_id):
        """extract, transform, and load into target protobuf"""

        # target protobuf
        target = bounty_searcher_pb2.BountySearcherResponse()


        # tweepy doesnt support asyncio, run in single threaded pool for now
        logger.info("extracting tweets")

        get_tweets = asyncio.get_running_loop().run_in_executor(self._tweepyExecutor, self._tweet_statuses_loader, since_id, max_id)

        try:        
            tweets, max_since_id = await asyncio.wait_for(get_tweets, TWEEPY_TIMEOUT)
        except asyncio.TimeoutError:
            logger.exception(f"timed out waiting for tweepy. returning empty")

            # we didn't process anything
            target.processed_count = 0

            # we pass the same max id as the input, so that if caller uses it to requery it picks up where it left off
            if since_id:
                target.max_id = since_id

            return target

        logger.debug(f"finished extracting {len(tweets)}, max id is {max_since_id}")

        # perform batch NLP analysis pipelines

        logger.info("batch processing tweets with nlp pipeline")

        text_list = (tweet['text'] for tweet in tweets)
        for doc, tweet in zip(self._nlp.pipe(text_list), tweets):
            token_count = Counter()
            for token in doc:
                if not token.is_stop and not token.is_punct and not token.is_space:
                    token_count[token.lemma_] += 1

            tweet['token_count'] = token_count

            ner_count = Counter()
            for ent in doc.ents:
                ner_count[ent.text] += 1

            tweet['ner_count'] = ner_count

            # regex based distribution
            regex_count = Counter()
            for match in re.finditer(REGEX_PATTERN, doc.text):
                start, end = match.span()
                #ignore the first character since it is $ or #
                span = doc.char_span(start+1, end)
                # This is a Span object or None if match doesn't map to valid token sequence

                if span is not None:
                    regex_count[span.text] += 1

            tweet['regex_count'] = regex_count

        logger.debug("finished nlp pipeline")


        # Write tweets to file object
        avro_file = BytesIO()
        writer(avro_file, PARSED_SCHEMA, tweets, validator=True, codec='deflate')

        # Write file object to gcs
        bucket = self._storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"tweets_{max_since_id}.avro")
        blob.upload_from_file(avro_file, rewind=True)

        logger.info(f"file tweets_{max_since_id}.avro uploaded to {BUCKET_NAME}")

        # load remaining target protobuf fields
        target.max_id = max_since_id
        target.processed_count = len(tweets)

        return target

def transform_helper(status):
    """Helper function that transforms extracted tweets to a dict suitable for loading into protobuf"""
    tweet = { 
        "screen_name" : status.user.screen_name,
        "display_name" : status.user.name,
        "user_id" : str(status.user.id),
        "is_retweet" : hasattr(status, 'retweeted_status'),
        "text" : status.full_text,
        "id" : int(status.id),
        "contains_media" : 'media' in status.entities,
        "client_type" : status.source,
        "time" : int(status.created_at.timestamp() * 1000), # timestamp in millis as per avro spec
        "date" : int((status.created_at - EPOCH).days)
        }

    # retweet text isnt populated directly, handle it
    if hasattr(status, 'retweeted_status'):
        tweet["text"] = status.retweeted_status.full_text

    return tweet

def transform(statuses):
    """returns generator that transforms raw tweepy status objects for ease of loading into target protobuf"""
    return (transform_helper(status) for status in statuses)

#-----------------------------------------------------------------
# Control Plane

async def serve(credentials):
    logger.info('starting up BountySearcherService')
    server = aio.server()
    service = BountySearcherService(credentials)
    bounty_searcher_pb2_grpc.add_BountySearcherServicer_to_server(service, server)
    server.add_insecure_port(f'[::]:{SERVICE_PORT}')
    try:
        await server.start()
        logger.info('service up at port %s. Max tweets per request set to %d', SERVICE_PORT, MAX_HOME_TIMELINE_TWEETS_PER_REQUEST)
        await server.wait_for_termination()
    except:
        # unknown exception, shut down server since we are in inconsistent state
        logger.exception("exception while servicing")        
        await server.stop(None)


if __name__ == '__main__':
    # Configuring logger
    logging.basicConfig(format=LOG_FORMATTER)
    logger = logging.getLogger(APPLICATION_BASE_NAME)
    logger.setLevel(logging.DEBUG)

    # parse credentials
    parser = argparse.ArgumentParser(description='BountySearcher Service')
    parser.add_argument('cred_file', type=str, help='twitter credentials file in json format')
    args = parser.parse_args()

    credentials = None
    with open(args.cred_file) as f:
        credentials = json.load(f)

    aio.init_grpc_aio()
    logging.basicConfig(level=logging.DEBUG)
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(serve(credentials))
    finally:
        loop.close()

