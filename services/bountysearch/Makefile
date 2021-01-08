SHELL=/bin/bash

.PHONY: build docker-build docker-run-local

PROJECT:= hack
REGION:= us-central1
SERVICE:= bountysearch
PORT:= 50051
NLPMODEL:= en_core_web_sm
ENVFILE:= dev.env

REDISHOST:=10.48.240.91
REDISPORT:=6379

VPC_CONNECTOR:=vpc-connector
VPC_PROJECT:=hack

CF_SERVICE_ACCOUNT:= bountysearch-invoker@hack.iam.gserviceaccount.com
CF_BUCKET_NAME:= bountysearch
CF_DATASET_ID:=bountysearch
CF_DATA_PROJECT_NAME:=data-285118
CF_GRPC_SERVICE:=bountysearch-uc.a.run.app:443
CF_GRPC_SERVICE_URL:=https://bountysearch-uc.a.run.app
CF_TABLE_NAME:=tweets

REPO=$(shell basename `git rev-parse --show-toplevel`)

docker-run-local: $(ENVFILE) creds.json
	docker run --env-file $(ENVFILE) --publish $(PORT):$(PORT) $(PROJECT)/$(SERVICE) python BountySearcher.py 
	
docker-build:
	cp ../../protos/services/${SERVICE}/*pb2*.py .
	docker build -t $(PROJECT)/$(SERVICE) --build-arg NLP_MODEL=$(NLPMODEL) .
	rm *pb2*.py

build:
	cp ../../protos/services/${SERVICE}/*pb2*.py .
	
	gcloud builds submit --project $(PROJECT) \
	--verbosity debug \
	--substitutions _NLPMODEL="$(NLPMODEL)",_SERVICE="$(SERVICE)",REPO_NAME="$(REPO)",SHORT_SHA="$(shell git rev-parse --short HEAD)" \
	--config cloudbuild.yaml

	
	rm *pb2*.py

encrypt-secrets:
	gcloud kms encrypt \
		--key main \
		--keyring bountysearch \
		--location global  \
		--plaintext-file creds.json \
		--ciphertext-file encrypted-$(PROJECT).json.enc

deploy:
	gcloud run deploy $(SERVICE) \
		--image gcr.io/$(PROJECT)/$(SERVICE) \
		--project $(PROJECT) \
		--region $(REGION) \
		--platform managed \
		--port $(PORT) \
		--concurrency 1 \
		--max-instances 1 \
		--no-allow-unauthenticated \
		--update-env-vars SERVICE_PORT="$(PORT)",MAX_HOME_TIMELINE_TWEETS_PER_REQUEST="200",NLP_MODEL="en_core_web_sm",REGEX_PATTERN="(#|\$$)[a-zA-Z0-9_]+"


deploy-cloud-function:
	rm -f ../../cloudfunctions/requestBountysearch/*pb2*.py
	cp ../../protos/services/${SERVICE}/*pb2*.py ../../cloudfunctions/requestBountysearch
	cd ../../cloudfunctions/requestBountysearch && \
	gcloud functions deploy requestBountysearch \
	--service-account=$(CF_SERVICE_ACCOUNT) \
	--project $(PROJECT) \
	--region $(REGION) \
	--vpc-connector projects/$(VPC_PROJECT)/locations/$(REGION)/connectors/$(VPC_CONNECTOR) \
	--entry-point requestBountysearch \
	--runtime python38 \
	--trigger-http \
	--update-env-vars PROJECT_NAME="$(PROJECT)",BUCKET_NAME="$(CF_BUCKET_NAME)",\
	DATASET_ID="$(CF_DATASET_ID)",DATA_PROJECT_NAME="$(CF_DATA_PROJECT_NAME)",\
	GRPC_SERVICE="$(CF_GRPC_SERVICE)",GRPC_SERVICE_URL="$(CF_GRPC_SERVICE_URL)",\
	REDISHOST="$(REDISHOST)",REDISPORT="$(REDISPORT)",TABLE_NAME="$(CF_TABLE_NAME)"

