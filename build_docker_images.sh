#!/bin/bash

# Build docker images
docker build -f "activity_node/Dockerfile" -t activity_node:latest "activity_node"
docker build -f "central_node/Dockerfile" -t central_node:latest "central_node"
