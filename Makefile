DOCKER_ACCOUNT = barcodepro
SITENAME = weaponry
APPNAME = agent
BINNAME = ${SITENAME}-${APPNAME}
IMAGENAME = ${APPNAME}-distribution

COMMIT=$(shell git rev-parse --short HEAD)
BRANCH=$(shell git rev-parse --abbrev-ref HEAD)

LDFLAGS = -a -installsuffix cgo -ldflags "-X main.binName=${BINNAME} -X main.appName=${APPNAME} -X main.COMMIT=${COMMIT} -X main.BRANCH=${BRANCH}"
DESTDIR ?=

.PHONY: help clean build docker-build docker-push deploy

.DEFAULT_GOAL := help

help:
	@echo "Makefile available targets:"
	@echo "  * clean                 remove program executable"
	@echo "  * build                 build program executable"
	@echo "  * docker-build          build Docker image"
	@echo "  * docker-push           push Docker image to Registry"
	@echo "  * deploy -e ENV=env     deploy to Kubernetes"

clean:
	rm -f bin/${BINNAME} bin/${BINNAME}.tar.gz

build:
	go mod download
	CGO_ENABLED=0 GOOS=linux GOARCH=${GOARCH} go build ${LDFLAGS} -o bin/${BINNAME} ./cmd/app
	tar czf ./bin/${BINNAME}.tar.gz -C ./bin ${BINNAME}

docker-build:
	docker build -t ${DOCKER_ACCOUNT}/${SITENAME}-${IMAGENAME}:${COMMIT} .
	docker image prune --force --filter label=stage=intermediate

docker-push:
	docker push ${DOCKER_ACCOUNT}/${SITENAME}-${IMAGENAME}:${COMMIT}

deploy:
	ansible-playbook deployment/ansible/deploy.yml -e env=${ENV}
