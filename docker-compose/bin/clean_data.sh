#!/bin/bash
current_dir=$(dirname "$(readlink -f "$0")")

parent_dir=$(dirname "$current_dir")

rm -fr "$parent_dir/services/blockscout-db-data" "$parent_dir/services/redis-data" "$parent_dir/services/stats-db-data" "$parent_dir/services/logs"
