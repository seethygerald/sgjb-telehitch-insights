#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  ./deploy/aws-ec2/check-ssh.sh <public-ip-or-dns> [private-key]

Example:
  ./deploy/aws-ec2/check-ssh.sh \
    ec2-203-0-113-10.ap-southeast-1.compute.amazonaws.com \
    ~/.ssh/telehitch-airflow.pem

Run this script on the computer that stores the EC2 private key, not on EC2.
It validates the local key and then attempts an Ubuntu SSH login with verbose
public-key authentication diagnostics.
USAGE
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage >&2
  exit 2
fi

host=$1
key_path=${2:-"$HOME/.ssh/telehitch-airflow.pem"}

if [[ "$host" == *"YOUR_"* || "$host" == "MY_EC2_PUBLIC_DNS" ]]; then
  echo "Error: replace the placeholder with the instance's actual Public IPv4 DNS or address." >&2
  exit 2
fi

if [[ ! -f "$key_path" ]]; then
  echo "Error: private key not found: $key_path" >&2
  echo "Find it with: find ~/.ssh ~/Downloads -maxdepth 2 -name '*.pem' -print" >&2
  exit 1
fi

permissions=$(stat -f '%Lp' "$key_path" 2>/dev/null || stat -c '%a' "$key_path")
if [[ "$permissions" != "400" ]]; then
  echo "Error: $key_path has permissions $permissions; SSH private keys should use 400." >&2
  echo "Run: chmod 400 '$key_path'" >&2
  exit 1
fi

if ! ssh-keygen -y -f "$key_path" >/dev/null 2>&1; then
  echo "Error: $key_path is not a readable OpenSSH private key." >&2
  exit 1
fi

echo "Local private key: $key_path"
echo "Derived public-key fingerprint:"
ssh-keygen -y -f "$key_path" | ssh-keygen -lf -
echo
echo "Before interpreting a publickey failure, open EC2 > Instances > your instance"
echo "and verify that 'Key pair name' is the key pair whose private key is shown above."
echo "Creating a new key pair after launch does not add it to an existing instance."
echo
echo "Attempting: ubuntu@$host"
echo

exec ssh -vvv \
  -o BatchMode=yes \
  -o ConnectTimeout=10 \
  -o IdentitiesOnly=yes \
  -i "$key_path" \
  "ubuntu@$host"
