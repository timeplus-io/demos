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
	docker run -it --rm -p 8080:8080 $(FULLNAME) 