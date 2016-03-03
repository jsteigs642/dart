#!/bin/sh

./create-repo.sh dart-cloudwatchlogs
./create-repo.sh dart-elasticmq
./create-repo.sh dart-engine-no_op
./create-repo.sh dart-engine-emr
./create-repo.sh dart-engine-redshift
./create-repo.sh dart-engine-worker
./create-repo.sh dart-flask
./create-repo.sh dart-nginx
./create-repo.sh dart-subscription-worker
./create-repo.sh dart-trigger-worker

# these are for the emr_engine
./create-repo.sh impala-state-store
./create-repo.sh impala-catalog
./create-repo.sh impala-server
