VERSION = $(shell git rev-parse --short HEAD)
BIN_NAME = marimo
IMAGE_NAME = $(BIN_NAME):$(VERSION)
DOCKER_ID_USER = timeplus
FULLNAME=$(DOCKER_ID_USER)/${IMAGE_NAME}

docker: Dockerfile
	docker buildx build \
		--no-cache -t $(FULLNAME) \
		--platform linux/arm64,linux/amd64 \
		--builder container \
		--push .

run:
	docker run -it --rm -e DB_HOST=$(DB_HOST) \
		-e DB_USER=$(DB_USER) -e DB_PASSWORD=$(DB_PASSWORD) \
		-e DB_NAME=$(DB_NAME) -e DB_PORT=$(DB_PORT) $(FULLNAME) 