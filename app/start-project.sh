parent_dir=$(dirname "$(pwd)")
cd "$parent_dir" || exit
docker-compose up -d
docker build --no-cache --rm -t bde/spark-app2 .
docker run --name spark-stream --net bde -p 4040:4040 -d bde/spark-app2
