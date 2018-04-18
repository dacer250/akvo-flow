#!/usr/bin/env bash

set -euo pipefail

CLOUD_SDK_VERSION="${CLOUD_SDK_VERSION:=196.0.0}"

mkdir "tmp"

docker run --rm \
       --interactive \
       --tty \
       --volume "$(pwd):/akvo-flow" \
       --volume "$(pwd)/tmp:/tmp" \
       --workdir "/akvo-flow" \
       --env GH_USER \
       --env GH_TOKEN \
       "google/cloud-sdk:${CLOUD_SDK_VERSION}" \
       "/akvo-flow/scripts/deploy/deploy.sh" "$@"
