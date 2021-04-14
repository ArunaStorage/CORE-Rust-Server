docker-compose --file docker-compose.test.yml build
docker-compose --file docker-compose.test.yml run --rm test
docker-compose --file docker-compose.test.yml rm -s -f