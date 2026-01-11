#!/bin/bash
git pull
docker stop grand_exchange_sync
docker container rm grand_exchange_sync
docker compose up -d
