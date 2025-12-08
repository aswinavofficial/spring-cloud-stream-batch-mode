#!/bin/sh
# Maven Wrapper script

set -e

MAVEN_PROJECTBASEDIR="${MAVEN_BASEDIR:-$(cd "$(dirname "$0")" && pwd)}"
MAVEN_WRAPPER_PROPERTIES="$MAVEN_PROJECTBASEDIR/.mvn/wrapper/maven-wrapper.properties"

if [ -r "$MAVEN_WRAPPER_PROPERTIES" ]; then
    while IFS='=' read -r key value || [ -n "$key" ]; do
        key=$(echo "$key" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        case "$key" in
            distributionUrl) MVNW_DIST_URL="$value" ;;
        esac
    done < "$MAVEN_WRAPPER_PROPERTIES"
fi

MVNW_DIST_URL="${MVNW_DIST_URL:-https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.9.6/apache-maven-3.9.6-bin.zip}"
MAVEN_HOME="${MAVEN_HOME:-$HOME/.m2/wrapper/dists/apache-maven-3.9.6}"

if [ ! -f "$MAVEN_HOME/bin/mvn" ]; then
    echo "Downloading Maven..."
    mkdir -p "$MAVEN_HOME"
    TEMP_FILE=$(mktemp)
    curl -sL "$MVNW_DIST_URL" -o "$TEMP_FILE"
    unzip -q "$TEMP_FILE" -d "$MAVEN_HOME/.."
    rm "$TEMP_FILE"
fi

exec "$MAVEN_HOME/bin/mvn" "$@"
