#!/usr/bin/env bash
set -euo pipefail

WG_CONF="/home/btckali/Downloads/br-sao.conf"
WG_IFACE="br-sao"

sudo -n wg-quick down "$WG_IFACE" >/dev/null 2>&1 || true
sudo -n wg-quick up "$WG_CONF"

wg show interfaces
curl -s https://ipinfo.io/json | grep -E '"ip"|"city"|"country"'
