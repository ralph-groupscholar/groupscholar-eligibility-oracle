#!/bin/sh
set -e
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
mkdir -p build
javac -d build src/EligibilityOracle.java
java -cp build EligibilityOracle "$@"
