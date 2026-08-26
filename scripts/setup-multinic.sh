#!/usr/bin/env bash
# Layer-1 setup for multi-NIC S3 throughput (run as root ON the EC2 instance).
#
# For each SECONDARY ENI (any up, non-loopback interface that is not the primary
# default-route interface), install SOURCE-BASED POLICY ROUTING so that a socket
# bound to that ENI's IP actually egresses — and returns on — that ENI's network
# card. Without this, binding a source IP (the Go dialer's LocalAddr trick) still
# egresses the primary card and/or the returns get dropped by rp_filter, so you
# get no multi-NIC benefit.
#
# Idempotent: if an ENI already has a `from <ip>` rule (e.g. amazon-ec2-net-utils
# configured it), it is left alone. Safe to re-run.
set -euo pipefail

PRIMARY_IF=$(ip -o route show default | awk '{print $5; exit}')
echo ">> primary interface (default route): ${PRIMARY_IF:-<none>}"

gw_of() { # arg: CIDR (e.g. 172.31.56.9/20) -> subnet base + 1
  python3 -c "import ipaddress,sys;print(str(ipaddress.ip_interface(sys.argv[1]).network.network_address+1))" "$1"
}
net_of() { python3 -c "import ipaddress,sys;print(str(ipaddress.ip_interface(sys.argv[1]).network))" "$1"; }

TABLE_BASE=1000
count=0
for ifc in $(ls /sys/class/net | grep -vE '^(lo|docker|veth|br-|virbr)'); do
  [ "$ifc" = "$PRIMARY_IF" ] && continue

  # ensure it's up
  [ "$(cat /sys/class/net/"$ifc"/operstate 2>/dev/null)" = "up" ] || ip link set "$ifc" up 2>/dev/null || true

  cidr=$(ip -o -4 addr show dev "$ifc" | awk '{print $4; exit}')
  if [ -z "$cidr" ]; then
    dhclient "$ifc" 2>/dev/null || true
    cidr=$(ip -o -4 addr show dev "$ifc" | awk '{print $4; exit}')
  fi
  [ -n "$cidr" ] || { echo ">> skip $ifc (no IPv4 assigned)"; continue; }
  ip4=${cidr%/*}

  if ip rule show | grep -q "from $ip4 "; then
    echo ">> $ifc ($ip4): policy routing already present — leaving as is"
    count=$((count + 1))
    continue
  fi

  gw=$(gw_of "$cidr"); net=$(net_of "$cidr")
  count=$((count + 1))
  table=$((TABLE_BASE + count))
  echo ">> $ifc ip=$ip4 cidr=$cidr gw=$gw -> table $table"
  ip route flush table "$table" 2>/dev/null || true
  ip route add "$net" dev "$ifc" src "$ip4" table "$table"
  ip route add default via "$gw" dev "$ifc" table "$table"
  ip rule add from "$ip4" lookup "$table"
  sysctl -qw "net.ipv4.conf.$ifc.rp_filter=2" || true
done

sysctl -qw net.ipv4.conf.all.rp_filter=2 || true
echo ">> ip rules:"; ip rule show
echo ">> configured $count secondary ENI(s)"
