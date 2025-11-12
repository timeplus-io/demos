
# Get git commit hash (short version)
GIT_HASH=$(shell git rev-parse --short HEAD 2>/dev/null || echo "dev")
# Get full git commit hash (optional, if you want the full hash)
GIT_HASH_FULL=$(shell git rev-parse HEAD 2>/dev/null || echo "dev")

BIN_NAME ?= alert-ui
VERSION ?= 0.1
IMAGE_NAME ?= $(BIN_NAME):$(GIT_HASH)
DOCKER_ID_USER ?= timeplus



# Image name with git hash
FULLNAME=$(DOCKER_ID_USER)/$(IMAGE_NAME)

PWD=$(shell pwd)

.PHONY: docker docker_run push run build clean info

all: run 

run:
	go run main.go

build:
	go build -o $(BIN_NAME) main.go

docker: Dockerfile
	docker build -t $(IMAGE_NAME) .
	docker tag $(IMAGE_NAME) $(FULLNAME)

docker_run:
	docker run -p 8080:8080 $(IMAGE_NAME)

push:
	docker tag $(IMAGE_NAME) $(FULLNAME)
	docker push $(FULLNAME)
	@echo "Pushed: $(FULLNAME)"

# Push with latest tag as well
push_latest: push
	docker tag $(IMAGE_NAME) $(DOCKER_ID_USER)/$(BIN_NAME):latest
	docker push $(DOCKER_ID_USER)/$(BIN_NAME):latest
	@echo "Pushed: $(DOCKER_ID_USER)/$(BIN_NAME):latest"

# Show current tags that would be used
info:
	@echo "Binary name:      $(BIN_NAME)"
	@echo "Version:          $(VERSION)"
	@echo "Git hash (short): $(GIT_HASH)"
	@echo "Git hash (full):  $(GIT_HASH_FULL)"
	@echo "Local image:      $(IMAGE_NAME)"
	@echo "Remote image:     $(FULLNAME)"

clean:
	rm -f $(BIN_NAME)
	docker rmi -f $(IMAGE_NAME) $(FULLNAME) 2>/dev/null || true