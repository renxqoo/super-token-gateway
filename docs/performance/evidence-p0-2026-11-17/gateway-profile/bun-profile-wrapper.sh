#!/bin/sh
case "$1" in
  */dist/gateway/main.js) exec /Users/wr/work/super-token-gateway/.local/toolchain/bun-1.4.4/bun --cpu-prof --cpu-prof-md --cpu-prof-interval=1000 --cpu-prof-dir=/Users/wr/work/super-token-gateway/.local/p0-evidence/gateway-profile "$@" ;;
  *) exec /Users/wr/work/super-token-gateway/.local/toolchain/bun-1.4.4/bun "$@" ;;
esac
