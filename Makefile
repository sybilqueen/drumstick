PROJECT:=hack-285118

.PHONY: compile-protos clean-protos

compile-protos:
	find ./protos -name "*proto" -execdir python -m grpc_tools.protoc --proto_path=. --python_out=. --grpc_python_out=. {} \;

clean-protos:
	find ./protos -name "*pb2*.py" -exec rm {} \;


encrypt-util:
	gcloud kms encrypt \
		--key util \
		--project $(PROJECT) \
		--keyring main \
		--location global  \
		--plaintext-file utils/carrot.json \
		--ciphertext-file utils/encrypted-carrot.json.enc

	gcloud kms encrypt \
		--key util \
		--project $(PROJECT) \
		--keyring main \
		--location global  \
		--plaintext-file utils/functions.fish \
		--ciphertext-file utils/encrypted-functions.fish.enc

decrypt-util:
	gcloud kms decrypt \
		--key util \
		--project $(PROJECT) \
		--keyring main \
		--location global  \
		--plaintext-file utils/carrot.json \
		--ciphertext-file utils/encrypted-carrot.json.enc

	gcloud kms decrypt \
		--key util \
		--project $(PROJECT) \
		--keyring main \
		--location global  \
		--plaintext-file utils/functions.fish \
		--ciphertext-file utils/encrypted-functions.fish.enc

everything-is-committed:
	everything_is_committed

