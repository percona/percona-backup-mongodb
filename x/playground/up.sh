#!/usr/bin/env bash
#
# Bring up the sharded MongoDB cluster, then launch the agent playground.
#
set -euo pipefail

cd "$(dirname "$0")"

for bin in goreman docker mongosh; do
	command -v "$bin" >/dev/null 2>&1 || {
		echo "error: '$bin' not found in PATH" >&2
		exit 1
	}
done

if [[ "${SKIP_CLUSTER:-}" != "1" ]]; then
	echo ">> bringing up sharded MongoDB cluster ..."
    # start your mongo cluster here
	(cd ../docker/sharded && ./init.sh)
fi

echo ">> starting agents (goreman)..."
exec goreman start
