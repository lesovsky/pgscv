DOCKER_ACCOUNT = barcodepro
APPNAME = pgscv
IMAGENAME = weaponry-${APPNAME}-distribution

COMMIT=$(shell git rev-parse --short HEAD)
BRANCH=$(shell git rev-parse --abbrev-ref HEAD)

LDFLAGS = -a -installsuffix cgo -ldflags "-X main.appName=${APPNAME} -X main.gitCommit=${COMMIT} -X main.gitBranch=${BRANCH}"

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
	golangci-lint run --timeout 5m -E golint -e '(method|func) [a-zA-Z]+ should be [a-zA-Z]+'

test: dep ## Run unittests
	go test -short -timeout 300s -p 1 ./...

race: dep ## Run data race detector
	go test -race -short -timeout 300s -p 1 ./...

#coverage: ## Generate global code coverage report
#  ./tools/coverage.sh;
#
#coverhtml: ## Generate global code coverage report in HTML
#  ./tools/coverage.sh html;

build: dep ## Build
	mkdir -p ./bin
	CGO_ENABLED=0 GOOS=linux GOARCH=${GOARCH} go build ${LDFLAGS} -o bin/${APPNAME} ./service/cmd
	cd bin; \
		tar czf ${APPNAME}.tar.gz ${APPNAME} && \
		sha256sum ${APPNAME}.tar.gz > ${APPNAME}.sha256 && \
		echo ${COMMIT}-${BRANCH} > ${APPNAME}.version

docker-build: ## Build docker image
	mkdir -p ./bin
	./extras/genscript.sh ${ENV} > ./bin/install.sh
	docker build -t ${DOCKER_ACCOUNT}/${IMAGENAME}:${COMMIT}-${ENV} .
	docker image prune --force --filter label=stage=intermediate
	rm ./bin/install.sh
	rmdir ./bin

docker-push: ## Push docker image
	docker push ${DOCKER_ACCOUNT}/${IMAGENAME}:${COMMIT}-${ENV}

deploy: ## Deploy
	ansible-playbook deployment/ansible/deploy.yml -e env=${ENV}

docker-build-test-runner: ## Build environmental docker image for CI tests
	$(eval VERSION := $(shell grep -E 'LABEL version' deployment/docker-test-runner/Dockerfile |cut -d = -f2 |tr -d \"))
	cd ./deployment/docker-test-runner; \
		docker build -t ${DOCKER_ACCOUNT}/weaponry-pgscv-test-runner:${VERSION} .

docker-push-test-runner: ## Build environmental docker image for CI tests
	$(eval VERSION := $(shell grep -E 'LABEL version' deployment/docker-test-runner/Dockerfile |cut -d = -f2 |tr -d \"))
	cd ./deployment/docker-test-runner; \
		docker push ${DOCKER_ACCOUNT}/weaponry-pgscv-test-runner:${VERSION}
