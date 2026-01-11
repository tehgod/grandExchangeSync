#!/bin/bash
git pull
docker stop grand-exchange-sync
docker container rm grand-exchange-sync
docker compose up -d
