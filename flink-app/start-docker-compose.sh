parent_dir=$(dirname "$(pwd)")
cd "$parent_dir" || exit
docker-compose -f docker-compose-flink.yml up -d
