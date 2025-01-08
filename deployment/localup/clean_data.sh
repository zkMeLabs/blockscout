#!/bin/bash
script_dir=$(dirname "$(readlink -f "$0")")
docker_compose_dir=$(realpath "$script_dir"/../../docker-compose)
rm -fr "$docker_compose_dir/services/blockscout-db-data" "$docker_compose_dir/services/redis-data" "$docker_compose_dir/services/stats-db-data" "$docker_compose_dir/services/logs"
