DOCKER_ACCOUNT = weaponry
APPNAME = pgscv

TAG=$(shell git describe --tags --abbrev=0)
COMMIT=$(shell git rev-parse --short HEAD)
BRANCH=$(shell git rev-parse --abbrev-ref HEAD)

LDFLAGS = -a -installsuffix cgo -ldflags "-X main.appName=${APPNAME} -X main.gitTag=${TAG} -X main.gitCommit=${COMMIT} -X main.gitBranch=${BRANCH}"

.PHONY: help \
		clean lint test race \
		build migrate docker-build docker-push deploy

.DEFAULT_GOAL := help

help: ## Display this help screen
	@echo "Makefile available targets:"
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  * \033[36m%-15s\033[0m %s\n", $$1, $$2}'

clean: ## Clean
	rm -f ./bin/${APPNAME} ./bin/${APPNAME}.tar.gz ./bin/${APPNAME}.version ./bin/${APPNAME}.sha256
	rmdir ./bin

dep: ## Get the dependencies
	go mod download

lint: ## Lint the source files
	golangci-lint run --timeout 5m -E golint -e '(struct field|type|method|func) [a-zA-Z`]+ should be [a-zA-Z`]+'
	gosec -quiet ./...

test: dep lint ## Run tests
	go test -race -timeout 300s -coverprofile=.test_coverage.txt ./... && \
    	go tool cover -func=.test_coverage.txt | tail -n1 | awk '{print "Total test coverage: " $$3}'
	@rm .test_coverage.txt

race: dep ## Run data race detector
	go test -race -short -timeout 300s -p 1 ./...

build: dep ## Build
	mkdir -p ./bin
	CGO_ENABLED=0 GOOS=linux GOARCH=${GOARCH} go build ${LDFLAGS} -o bin/${APPNAME} ./cmd

docker-build: ## Build docker image
	docker build -t ${DOCKER_ACCOUNT}/${APPNAME}:${TAG} .
	docker image prune --force --filter label=stage=intermediate
	docker tag ${DOCKER_ACCOUNT}/${APPNAME}:${TAG} ${DOCKER_ACCOUNT}/${APPNAME}:latest

docker-push: ## Push docker image
	docker push ${DOCKER_ACCOUNT}/${APPNAME}:${TAG}
	docker push ${DOCKER_ACCOUNT}/${APPNAME}:latest

docker-build-test-runner: ## Build docker image with testing environment for CI
	$(eval VERSION := $(shell grep -E 'LABEL version' testing/docker-test-runner/Dockerfile |cut -d = -f2 |tr -d \"))
	cd ./testing/docker-test-runner; \
		docker build -t ${DOCKER_ACCOUNT}/pgscv-test-runner:${VERSION} .

docker-push-test-runner: ## Push testing docker image to registry
	$(eval VERSION := $(shell grep -E 'LABEL version' testing/docker-test-runner/Dockerfile |cut -d = -f2 |tr -d \"))
	cd ./testing/docker-test-runner; \
		docker push ${DOCKER_ACCOUNT}/pgscv-test-runner:${VERSION}
