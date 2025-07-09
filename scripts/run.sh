#!/bin/sh
set -e
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
mkdir -p build
javac -d build src/EligibilityOracle.java
CP="build"
if [ -d "lib" ]; then
  CP="build:lib/*"
fi
java -cp "$CP" EligibilityOracle "$@"
