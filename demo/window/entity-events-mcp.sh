#!/usr/bin/env bash
# Starts the entity-events MCP server (mcp/entity-events) on stdio, for an agent such as Claude Code.
# Build it first with `make window-demo-build`. Settings come from the environment; the defaults point
# at the window demo's broker (see EntityEventsServer).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
jar="$here/../../mcp/entity-events/target/entity-events-mcp.jar"

# The agent may not inherit an interactive shell's PATH: find a JDK 17+ explicitly.
if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
  java="$JAVA_HOME/bin/java"
elif home="$(/usr/libexec/java_home -v 17+ 2>/dev/null)"; then
  java="$home/bin/java"
else
  java="java"
fi
exec "$java" -jar "$jar"
