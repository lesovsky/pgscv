# stage 1
# __release_tag__ golang 1.18 was released 2022-03-15
FROM golang:1.18 as build
LABEL stage=intermediate
WORKDIR /app
COPY . .
RUN make build

# stage 2: scratch
# __release_tag__ alpine 3.13 was released 2021-02-18
FROM alpine:3.13 as dist
COPY --from=build /app/bin/pgscv /bin/pgscv
CMD ["pgscv"]