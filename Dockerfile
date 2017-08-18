# 

FROM xuntian/npl-runtime
MAINTAINER xuntian "li.zq@foxmail.com"

RUN mkdir -p /root/TableDB
ADD ./ /root/TableDB

WORKDIR /root/TableDB
COPY docker-entrypoint.sh /usr/local/bin/
RUN ln -s usr/local/bin/docker-entrypoint.sh / # backwards compat
ENTRYPOINT ["docker-entrypoint.sh"]
