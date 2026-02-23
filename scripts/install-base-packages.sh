#!/bin/bash
# Update packages in the base Docker image for security patches.
# https://pythonspeed.com/articles/security-updates-in-docker/
set -euo pipefail
set -x
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get -y upgrade
apt-get -y install --no-install-recommends
    # Add runtime system packages here.
