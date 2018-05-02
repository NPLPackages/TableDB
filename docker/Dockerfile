# 

FROM xuntian/npl-runtime:latest

MAINTAINER xuntian "li.zq@foxmail.com"

RUN mkdir -p /app/tabledb/setup/server

ADD ./npl_mod /app/tabledb/npl_mod/
ADD ./npl_packages /app/tabledb/npl_packages/

WORKDIR /app/tabledb/setup/server

COPY ./init-cluster.json /app/tabledb/setup/server/cluster.json
COPY ./docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
RUN ln -s usr/local/bin/docker-entrypoint.sh / # backwards compat
ENTRYPOINT ["docker-entrypoint.sh"]
