#!/bin/bash
set -e  # Exit on command errors
set -x  # Print each command before execution, useful for debugging
cd /Users/plakhanpal/Documents/git/qdrant-native-app-spcs/qdrant-native-app-spcs/service/qdrant_base
docker build --rm --platform linux/amd64 -t native_app_qdrant_base .
cd ..
cd qdrant_primary
docker build --rm --platform linux/amd64 -t native_app_qdrant_primary .
cd ..
cd qdrant_secondary
docker build --rm --platform linux/amd64 -t native_app_qdrant_secondary .
cd ..
cd qdrant_driver
docker build --rm --platform linux/amd64 -t native_app_qdrant_driver .
cd ..
REPO_URL=$(snow spcs image-repository url qdrant_provider_db.qdrant_provider_schema.qdrant_provider_image_repo --role qdrant_provider_role)
echo $REPO_URL

docker tag native_app_qdrant_primary $REPO_URL/native_app_qdrant_primary
docker tag native_app_qdrant_secondary $REPO_URL/native_app_qdrant_secondary
docker tag native_app_qdrant_driver $REPO_URL/native_app_qdrant_driver

snow spcs image-registry login

docker push $REPO_URL/native_app_qdrant_primary
docker push $REPO_URL/native_app_qdrant_secondary
docker push $REPO_URL/native_app_qdrant_driver

snow spcs image-repository list-images qdrant_provider_db.qdrant_provider_schema.qdrant_provider_image_repo --role qdrant_provider_role

cd /Users/plakhanpal/Documents/git/qdrant-native-app-spcs/qdrant-native-app-spcs/