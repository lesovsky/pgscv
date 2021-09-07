# weaponry/pgscv-test-runner
# __release_tag__ postrges 13.3 was released 2021-08-12
# __release_tag__ golang 1.17 was released 2021-08-16
# __release_tag__ golangci-lint v1.42.1 was released 2021-09-07
# __release_tag__ gosec v2.8.1 was released 2021-06-17
FROM postgres:13.3

LABEL version="0.0.7"

# install dependencies
RUN apt-get update && \
    apt-get install -y make gcc curl pgbouncer && \
    curl -s -L https://golang.org/dl/go1.17.linux-amd64.tar.gz -o - | \
        tar xzf - -C /usr/local && \
    cp /usr/local/go/bin/go /usr/local/bin/ && \
    curl -s -L https://github.com/golangci/golangci-lint/releases/download/v1.42.1/golangci-lint-1.42.1-linux-amd64.tar.gz -o - | \
        tar xzf - -C /usr/local golangci-lint-1.42.1-linux-amd64/golangci-lint && \
    cp /usr/local/golangci-lint-1.42.1-linux-amd64/golangci-lint /usr/local/bin/ && \
    curl -s -L https://github.com/securego/gosec/releases/download/v2.8.1/gosec_2.8.1_linux_amd64.tar.gz -o - | \
        tar xzf - -C /usr/local/bin gosec && \
    mkdir /usr/local/testing/ && \
    rm -rf /var/lib/apt/lists/*

# copy prepare test environment scripts
COPY prepare-test-environment.sh /usr/local/bin/
COPY fixtures.sql /usr/local/testing/

CMD ["echo", "I'm weaponry pgscv test runner 0.0.7"]
