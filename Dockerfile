# stage 1
# __release_tag__ 1.13 was released 2019-09-03
FROM golang:1.13 as build-stage
LABEL stage=intermediate
WORKDIR /app
COPY . .
RUN make build

# stage 2
# __release_tag__ 1.17.8 was released 2020-01-21
FROM nginx:1.17.8
RUN rm /etc/nginx/nginx.conf /etc/nginx/conf.d/default.conf
COPY ./extras/nginx.conf /etc/nginx/nginx.conf
COPY ./extras/agent.conf /etc/nginx/conf.d/agent.conf
COPY ./bin/install.sh /var/www/html/install.sh
COPY --from=build-stage /app/bin/weaponry-agent.tar.gz /var/www/html/weaponry-agent.tar.gz
EXPOSE 1080
STOPSIGNAL SIGTERM
CMD ["nginx", "-g", "daemon off;"]
