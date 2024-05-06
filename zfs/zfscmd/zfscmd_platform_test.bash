#!/bin/sh
set -euo pipefail

echo "to stderr" 1>&2
echo "to stdout"

exit "$1"
