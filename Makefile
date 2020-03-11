DOCKER_ACCOUNT = barcodepro
SITENAME = weaponry
APPNAME = agent
BINNAME = ${SITENAME}-${APPNAME}
IMAGENAME = ${APPNAME}-distribution

COMMIT=$(shell git rev-parse --short HEAD)
BRANCH=$(shell git rev-parse --abbrev-ref HEAD)

LDFLAGS = -a -installsuffix cgo -ldflags "-X main.binName=${BINNAME} -X main.appName=${APPNAME} -X main.COMMIT=${COMMIT} -X main.BRANCH=${BRANCH}"
DESTDIR ?=

.PHONY: help clean dep lint test race build docker-build docker-push deploy

.DEFAULT_GOAL := help

help: ## Display this help screen
	@echo "Makefile available targets:"
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  * \033[36m%-15s\033[0m %s\n", $$1, $$2}'

clean: ## Clean
	rm -f bin/${BINNAME} bin/${BINNAME}.tar.gz

dep: ## Get the dependencies
	go mod download

lint: ## Lint the source files
	golint -set_exit_status ./...
	golangci-lint run --timeout 5m

test: dep ## Run unittests
	go test -short -timeout 300s ./...

race: dep ## Run data race detector
	go test -race -short -timeout 300s ./...

build: dep ## Build
	CGO_ENABLED=0 GOOS=linux GOARCH=${GOARCH} go build ${LDFLAGS} -o bin/${BINNAME} ./cmd/app
	tar czf ./bin/${BINNAME}.tar.gz -C ./bin ${BINNAME}

docker-build: ## Build docker image
	./extras/genscript.sh ${ENV} > ./bin/install.sh
	docker build -t ${DOCKER_ACCOUNT}/${SITENAME}-${IMAGENAME}:${COMMIT}-${ENV} .
	docker image prune --force --filter label=stage=intermediate
	rm ./bin/install.sh

docker-push: ## Push docker image
	docker push ${DOCKER_ACCOUNT}/${SITENAME}-${IMAGENAME}:${COMMIT}-${ENV}

deploy: ## Deploy
	ansible-playbook deployment/ansible/deploy.yml -e env=${ENV}
