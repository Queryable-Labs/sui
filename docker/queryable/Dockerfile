FROM ubuntu:jammy

ARG PROFILE=release

RUN apt-get update && apt-get install -y libjemalloc-dev

# Both bench and release profiles copy from release dir
COPY ./target/${PROFILE}/sui-node /usr/local/bin
COPY ./target/${PROFILE}/sui /usr/local/bin

RUN useradd -ms /bin/bash queryable

RUN mkdir /data
RUN mkdir /config

RUN chown queryable /data
RUN chown queryable /config

USER queryable

ARG BUILD_DATE
ARG GIT_REVISION
LABEL profile=$PROFILE
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION
