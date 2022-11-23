FROM debian:bullseye-slim AS runtime

ARG PROFILE=release

RUN apt-get update && apt-get install -y libjemalloc-dev

# Both bench and release profiles copy from release dir
COPY ./target/${PROFILE}/sui-node /usr/local/bin

ARG BUILD_DATE
ARG GIT_REVISION
LABEL profile=$PROFILE
LABEL build-date=$BUILD_DATE
LABEL git-revision=$GIT_REVISION
