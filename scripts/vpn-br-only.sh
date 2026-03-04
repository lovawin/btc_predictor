#!/usr/bin/env bash
set -euo pipefail

WG_CONF="/home/btckali/Downloads/br-sao.conf"
WG_IFACE="br-sao"
ATTEMPTS="${1:-3}"

for i in $(seq 1 "$ATTEMPTS"); do
  sudo -n wg-quick down "$WG_IFACE" >/dev/null 2>&1 || true
  sudo -n wg-quick up "$WG_CONF"

  COUNTRY="$(curl -4 -s https://ipinfo.io/country | tr -d '\r\n')"
  if [[ "$COUNTRY" == "BR" ]]; then
    echo "Connected to Brazil on attempt $i"
    curl -4 -s https://ipinfo.io/json | grep -E '"ip"|"city"|"region"|"country"|"org"'
    exit 0
  fi

  echo "Attempt $i not Brazil (country=$COUNTRY), retrying..."
  sudo -n wg-quick down "$WG_IFACE" >/dev/null 2>&1 || true
  sleep 1
done

echo "Failed to get Brazil exit after $ATTEMPTS attempts. VPN left disconnected."
exit 1
