#!/bin/bash

set -eu

docverse init
uvicorn docverse.main:app --host 0.0.0.0 --port 8080
