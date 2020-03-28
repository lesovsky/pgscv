FROM postgres:12

# install dependencies
RUN apt-get update && \
    apt-get install -y make gcc curl pgbouncer && \
    curl -s -L https://dl.google.com/go/go1.13.9.linux-amd64.tar.gz -o - | \
        tar xzf - -C /usr/local && \
    cp /usr/local/go/bin/go /usr/local/bin/ && \
    mkdir /usr/local/testing/ && \
    rm -rf /var/lib/apt/lists/*

# copy prepare test environment scripts
COPY prepare-test-environment.sh /usr/local/bin/
COPY fixtures.sql /usr/local/testing/

CMD ["echo", "I'm wepoanry-pgscv test runner"]
