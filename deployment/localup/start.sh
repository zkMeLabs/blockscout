#!/bin/bash
script_dir=$(dirname "$(readlink -f "$0")")
docker_compose_dir=$(realpath "$script_dir"/../../docker-compose)

docker compose -f "${docker_compose_dir}/docker-compose.yml" up --build -d
