build:
	mvn package -D skipTests

compile:
	mvn compile -D checkstyle.skip

up:
	docker-compose -f docker/docker-compose.yml up

cli:
	docker exec -it trino trino --catalog gsheets --schema default

clean:
	docker rm trino
