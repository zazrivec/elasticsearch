#!/bin/bash
set -ev

./gradlew --info --stacktrace clean build buildDockerImage publishDockerImageWithLatest -x test
