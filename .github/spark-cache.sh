#!/bin/bash
# Pull a pre-built Spark image from Docker Hub cache, or build locally.
# Sets SPARK_IMAGE env var for docker-compose.
#
# Usage: source .github/spark-cache.sh

set -euo pipefail

CACHE_REPO="${CACHE_REPO:-scylladb/migrator-cache}"
HASH=$(find dockerfiles/spark -type f | sort | xargs sha256sum | sha256sum | cut -d' ' -f1 | head -c 16)
IMAGE="${CACHE_REPO}:spark-${HASH}"

echo "Spark cache image: ${IMAGE}"

if docker pull "${IMAGE}" 2>/dev/null; then
  echo "Cache hit — pulled ${IMAGE}"
else
  echo "Cache miss — building from dockerfiles/spark"
  docker build -t "${IMAGE}" dockerfiles/spark

  # Push to cache if Docker Hub credentials are available
  if [ -n "${DOCKERHUB_USERNAME:-}" ] && [ -n "${DOCKERHUB_TOKEN:-}" ]; then
    echo "${DOCKERHUB_TOKEN}" | docker login -u "${DOCKERHUB_USERNAME}" --password-stdin
    docker push "${IMAGE}"
    echo "Pushed ${IMAGE} to cache"
  else
    echo "No Docker Hub credentials — skipping push"
  fi
fi

export SPARK_IMAGE="${IMAGE}"
echo "SPARK_IMAGE=${IMAGE}" >> "${GITHUB_ENV:-/dev/null}"
