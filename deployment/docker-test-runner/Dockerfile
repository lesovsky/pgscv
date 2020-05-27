# barcodepro/weaponry-pgscv-test-runner
# __release_tag__ postrges 12.3 was released 2020-05-14
# __release_tag__ golang 1.14.3 was released 2020-05-14
FROM postgres:12.3

LABEL version="v0.0.1"

# install dependencies
RUN apt-get update && \
    apt-get install -y make gcc curl pgbouncer && \
    curl -s -L https://dl.google.com/go/go1.14.3.linux-amd64.tar.gz -o - | \
        tar xzf - -C /usr/local && \
    cp /usr/local/go/bin/go /usr/local/bin/ && \
    mkdir /usr/local/testing/ && \
    rm -rf /var/lib/apt/lists/*

# copy prepare test environment scripts
COPY prepare-test-environment.sh /usr/local/bin/
COPY fixtures.sql /usr/local/testing/

CMD ["echo", "I'm weaponry-pgscv test runner"]
