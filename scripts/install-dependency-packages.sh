#!/bin/bash
# Install packages needed during the dependency build stage but not at runtime.
set -euo pipefail
set -x
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get -y install --no-install-recommends build-essential git
