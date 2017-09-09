#!/bin/sh

set -eu

# usage: docker-publish.sh [tag]

tag=""
if [ -n "${1:-}" ]; then
    tag="$1"
    registry="registry.cncf.ci/linkerd/linkerd"
fi

# if DOCKER_CREDENTIALS is set, save it locally.
if [ -n "${DOCKER_CREDENTIALS:-}" ]; then
  mkdir -p ~/.docker
  echo "$DOCKER_CREDENTIALS" > ~/.docker/config.json
fi

# For debugging, alow this to be run without pushing.
docker_target="dockerBuildAndPush"
if [ "${NO_PUSH:-}" = "1" ]; then
  docker_target="docker"
fi

if [ -n "$tag" ]; then
    ./sbt "set Base.dockerRegistry in (linkerd, Bundle) := \"${tag}\"" "set Base.dockerRegistry in (linkerd, Bundle) := \"${registry}\"" "linkerd/bundle:${docker_target}" \
          "set Base.dockerTag in (namerd, Bundle) := \"${tag}\"" "set Base.dockerRegistry in (linkerd, Bundle) := \"${registry}\"" "namerd/bundle:${docker_target}" \
          "set Base.dockerTag in (namerd, Dcos) := \"dcos-${tag}\"" "set Base.dockerRegistry in (linkerd, Bundle) := \"${registry}\"" "namerd/dcos:${docker_target}"
else
  ./sbt "linkerd/bundle:${docker_target}" \
        "namerd/bundle:${docker_target}" \
        "namerd/dcos:${docker_target}"
fi
