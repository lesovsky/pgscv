PROGRAM_NAME = pgscv
PREFIX ?= /usr
INCLUDEDIR =
LIBDIR =

SOURCES = $(wildcard *.go)
COMMIT=$(shell git rev-parse HEAD)
BRANCH=$(shell git rev-parse --abbrev-ref HEAD)

LDFLAGS = -ldflags "-X main.COMMIT=${COMMIT} -X main.BRANCH=${BRANCH}"

DESTDIR ?=

.PHONY: all clean install uninstall

all: pgscv

pgscv:
	go mod download
	CGO_ENABLED=0 GOOS=linux GOARCH=${GOARCH} go build ${LDFLAGS} -o ${PROGRAM_NAME} ${SOURCES}

install:
	mkdir -p ${DESTDIR}${PREFIX}/bin/
	install -pm 755 ${PROGRAM_NAME} ${DESTDIR}${PREFIX}/bin/${PROGRAM_NAME}

clean:
	rm -f ${PROGRAM_NAME}

uninstall:
	rm -f ${DESTDIR}${PREFIX}/bin/${PROGRAM_NAME}
