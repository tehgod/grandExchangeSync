#!/bin/bash
git pull
docker stop grand_exchange_sync
docker container rm grand_exchange_sync_image
docker build --no-cache -t grand_exchange_sync_image .
docker run -d --name grand_exchange_sync --restart unless-stopped grand_exchange_sync_image
