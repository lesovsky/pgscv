# lesovsky/pgscv-test-runner
# __release_tag__ postrges 14.2 was released 2022-02-10
# __release_tag__ golang 1.18 was released 2022-03-15
# __release_tag__ golangci-lint v1.45.2 was released 2022-03-24
# __release_tag__ gosec v2.11.0 was released 2022-03-21
FROM postgres:14.2

LABEL version="0.0.9"

# install dependencies
RUN apt-get update && \
    apt-get install -y make gcc git curl pgbouncer && \
    curl -s -L https://golang.org/dl/go1.18.linux-amd64.tar.gz -o - | tar xzf - -C /usr/local && \
    cp /usr/local/go/bin/go /usr/local/bin/ && \
    curl -s -L https://github.com/golangci/golangci-lint/releases/download/v1.45.2/golangci-lint-1.45.2-linux-amd64.tar.gz -o - | \
        tar xzf - -C /usr/local golangci-lint-1.45.2-linux-amd64/golangci-lint && \
    cp /usr/local/golangci-lint-1.45.2-linux-amd64/golangci-lint /usr/local/bin/ && \
    curl -s -L https://github.com/securego/gosec/releases/download/v2.11.0/gosec_2.11.0_linux_amd64.tar.gz -o - | \
        tar xzf - -C /usr/local/bin gosec && \
    mkdir /usr/local/testing/ && \
    rm -rf /var/lib/apt/lists/*

# copy prepare test environment scripts
COPY prepare-test-environment.sh /usr/local/bin/
COPY fixtures.sql /usr/local/testing/

CMD ["echo", "I'm pgscv test runner 0.0.9"]
