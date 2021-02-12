# weaponry/pgscv-test-runner
# __release_tag__ postrges 13.1 was released 2020-11-12
# __release_tag__ golang 1.15.6 was released 2020-12-03
# __release_tag__ golangci-lint v1.33.0 was released 2020-11-23
# __release_tag__ gosec v2.5.0 was released 2020-10-26
FROM postgres:13.1

LABEL version="v0.0.4"

# install dependencies
RUN apt-get update && \
    apt-get install -y make gcc curl pgbouncer && \
    curl -s -L https://dl.google.com/go/go1.15.6.linux-amd64.tar.gz -o - | \
        tar xzf - -C /usr/local && \
    cp /usr/local/go/bin/go /usr/local/bin/ && \
    curl -s -L https://github.com/golangci/golangci-lint/releases/download/v1.33.0/golangci-lint-1.33.0-linux-amd64.tar.gz -o - | \
        tar xzf - -C /usr/local golangci-lint-1.33.0-linux-amd64/golangci-lint && \
    cp /usr/local/golangci-lint-1.33.0-linux-amd64/golangci-lint /usr/local/bin/ && \
    curl -s -L https://github.com/securego/gosec/releases/download/v2.5.0/gosec_2.5.0_linux_amd64.tar.gz -o - | \
        tar xzf - -C /usr/local/bin gosec && \
    mkdir /usr/local/testing/ && \
    rm -rf /var/lib/apt/lists/*

# copy prepare test environment scripts
COPY prepare-test-environment.sh /usr/local/bin/
COPY fixtures.sql /usr/local/testing/

CMD ["echo", "I'm weaponry pgscv test runner v0.0.4"]
