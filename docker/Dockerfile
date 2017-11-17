# 

FROM xuntian/npl-runtime:20171103

MAINTAINER xuntian "li.zq@foxmail.com"

RUN mkdir -p /app/tabledb
RUN mkdir -p /app/tabledb/setup/server

# below copy need add link to the ../npl_mod and ../npl_packages dir
# in windows it's `mklink /J npl_packages ..\npl_packages\` 
ADD ./npl_mod  /app/tabledb/npl_mod/
ADD ./npl_packages  /app/tabledb/npl_packages/

WORKDIR /app/tabledb/setup/server

COPY ./init-cluster.json  /app/tabledb/setup/server/cluster.json
COPY ./docker-entrypoint.sh /usr/local/bin/
RUN ln -s usr/local/bin/docker-entrypoint.sh / # backwards compat
ENTRYPOINT ["docker-entrypoint.sh"]
