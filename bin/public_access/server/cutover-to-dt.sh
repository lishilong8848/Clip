#!/bin/bash
set -Eeuo pipefail
umask 077
base=/opt/clipflow-public
sites=/www/server/panel/vhost/nginx
exec 9>"$base/domain-cutover.lock"
flock -n 9 || exit 0
if [[ -f "$base/domain-dt-active" ]]; then
    systemctl disable --now clipflow-domain-cutover.timer
    exit 0
fi

# Wait for the updated designated host; never replay a business submission.
if ! response=$(curl --noproxy '*' --fail --silent --show-error --max-time 4 \
    -H 'Host: dt.sjhl.online' 'http://127.0.0.1:6100/api/health?probe=1' 2>/dev/null); then
    exit 0
fi
[[ "$response" == '{"ok":true,"service":"clipflow_backend"}' ]] || exit 0
openssl x509 -in "$base/tls/dt/fullchain.pem" -noout -checkend 0 -checkhost dt.sjhl.online
backup="$base/backups/domain-cutover.$(date +%Y%m%d%H%M%S)"
mkdir -p "$backup"
for domain in dt dengta; do
    cp -a "$sites/$domain.sjhl.online.conf" "$backup/$domain.conf"
done
restore() {
    status=$?
    trap - ERR
    for domain in dt dengta; do
        cp -a "$backup/$domain.conf" "$sites/$domain.sjhl.online.conf"
    done
    nginx -t && nginx -s reload
    exit "$status"
}
trap restore ERR
for domain in dt dengta; do
    install -m 0644 "$base/deployment/$domain-https.conf" "$sites/$domain.sjhl.online.conf"
done
nginx -t
nginx -s reload
touch "$base/domain-dt-active"
trap - ERR
systemctl disable --now clipflow-domain-cutover.timer
echo 'dt.sjhl.online is active; the old domain redirects to it.'
