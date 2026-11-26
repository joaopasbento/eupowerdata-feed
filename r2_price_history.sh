#!/usr/bin/env bash
# r2_price_history.sh - the Pro files of the price history in the private R2 bucket.
#
# The files of each zone and year and the daily means (data/price-history/<ZONE>/
# and data/price-history/daily/, see fetch_price_history.py) are sold in the Pro
# plan: since 26 Nov 2026 they are kept in the private Cloudflare R2 bucket
# eupowerdata-pro, under the same keys from price-history/ on (for example
# price-history/PT/2026.json), and not in this public repository. The index and
# the monthly means (data/price-history/index.json and monthly.json) stay public,
# in the repository, and are never sent to R2.
#
#   ./r2_price_history.sh down            get the Pro files from R2 into data/price-history/
#   ./r2_price_history.sh up              send the ones that changed, then check (below)
#   ./r2_price_history.sh check           fail unless every local Pro file is in R2 as it is here
#   ./r2_price_history.sh verify-content  download all of them apart and compare them byte by byte
#
# With the AWS CLI of the GitHub runners (no third-party action), and the token of
# the repository's secrets R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY, which only
# reads and writes objects of that bucket. Any failure ends the step in error.
set -euo pipefail

: "${AWS_ACCESS_KEY_ID:?the secret R2_ACCESS_KEY_ID is not set}"
: "${AWS_SECRET_ACCESS_KEY:?the secret R2_SECRET_ACCESS_KEY is not set}"
export AWS_DEFAULT_REGION=auto
# R2 does not take every checksum header recent AWS CLIs send by default.
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required
export AWS_RESPONSE_CHECKSUM_VALIDATION=when_required

ENDPOINT='https://6187e69a7e7186410b61ab5bc9c79105.r2.cloudflarestorage.com'
REMOTE='s3://eupowerdata-pro/price-history/'
LOCAL='data/price-history/'
PUBLIC=(--exclude 'index.json' --exclude 'monthly.json' --exclude '*.tmp' --exclude '.*')

pro_files() {
  find "$LOCAL" -mindepth 2 -name '*.json' | sort
}

check() {
  local pending
  pending=$(aws s3 sync "$LOCAL" "$REMOTE" --endpoint-url "$ENDPOINT" --dryrun "${PUBLIC[@]}")
  if [ -n "$pending" ]; then
    echo "::error::Pro files of the price history not in R2 as they are here:"
    echo "$pending"
    exit 1
  fi
  echo "R2: all $(pro_files | wc -l | tr -d ' ') Pro files of the price history are there as they are here"
}

case "${1:-}" in
  down)
    mkdir -p "$LOCAL"
    aws s3 sync "$REMOTE" "$LOCAL" --endpoint-url "$ENDPOINT" --only-show-errors "${PUBLIC[@]}"
    echo "R2: got $(pro_files | wc -l | tr -d ' ') Pro files of the price history"
    ;;
  up)
    aws s3 sync "$LOCAL" "$REMOTE" --endpoint-url "$ENDPOINT" "${PUBLIC[@]}" | sed 's/^/  /'
    check
    ;;
  check)
    check
    ;;
  verify-content)
    apart=$(mktemp -d)
    aws s3 sync "$REMOTE" "$apart/" --endpoint-url "$ENDPOINT" --only-show-errors "${PUBLIC[@]}"
    here=$(pro_files | wc -l | tr -d ' ')
    there=$(cd "$apart" && find . -mindepth 2 -name '*.json' | wc -l | tr -d ' ')
    if ! diff -rq --exclude index.json --exclude monthly.json "$LOCAL" "$apart/"; then
      echo "::error::the Pro files in R2 differ from those here"
      exit 1
    fi
    if [ "$here" != "$there" ]; then
      echo "::error::$here Pro files here, $there in R2"
      exit 1
    fi
    echo "R2: the $there Pro files of the price history are byte for byte those here"
    ;;
  *)
    echo "usage: $0 down | up | check | verify-content" >&2
    exit 2
    ;;
esac
