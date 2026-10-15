#!/bin/bash
set -Eeuo pipefail

domain=dt.sjhl.online
base=/opt/clipflow-public
source_dir="$base/deployment"
site="/www/server/panel/vhost/nginx/$domain.conf"

[[ $EUID == 0 ]] || { echo 'Run this script as root in the server terminal.'; exit 1; }
for file in dt-https.conf clipflow-cert-renew.service clipflow-cert-renew.timer; do
    test -s "$source_dir/$file"
done
test -s "$site"
test -s "$base/acme/acme.sh"
test -s "$base/tls/dt/privkey.pem"
openssl x509 -in "$base/tls/dt/fullchain.pem" -noout -checkend 0 -checkhost "$domain"
systemctl is-active --quiet firewalld
[[ $(firewall-cmd --get-zone-of-interface=eth0) == public ]]
nginx -t

# Restrict only 6100; never reload the firewall or change shared FRPS settings.
rules=(
    'rule family="ipv4" priority="-10" source not address="127.0.0.0/8" port port="6100" protocol="tcp" reject'
    'rule family="ipv6" priority="-10" source not address="::1/128" port port="6100" protocol="tcp" reject'
)
for mode in runtime permanent; do
    options=()
    [[ $mode != permanent ]] || options=(--permanent)
    for rule in "${rules[@]}"; do
        if ! firewall-cmd "${options[@]}" --zone=public --query-rich-rule="$rule" >/dev/null; then
            firewall-cmd "${options[@]}" --zone=public --add-rich-rule="$rule"
        fi
    done
done

install -d -m 0700 "$base/backups"
backup="$base/backups/dt-before-https.$(date +%Y%m%d%H%M%S).conf"
cp -a "$site" "$backup"
restore_site() {
    status=$?
    trap - ERR
    cp -a "$backup" "$site"
    nginx -t && nginx -s reload
    echo 'Activation failed. Restored the dedicated site; port isolation remains in place.' >&2
    exit "$status"
}
trap restore_site ERR

chmod 600 "$base/tls/dt/privkey.pem"
install -m 0644 "$source_dir/dt-https.conf" "$site"
nginx -t
nginx -s reload
install -m 0644 "$source_dir/clipflow-cert-renew.service" "$source_dir/clipflow-cert-renew.timer" /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now clipflow-cert-renew.timer
systemctl start clipflow-cert-renew.service
systemctl is-active --quiet clipflow-cert-renew.timer

# An offline FRP upstream may return 502; certificate verification must succeed.
verified=false
for attempt in {1..20}; do
    if curl --noproxy '*' --resolve "$domain:443:127.0.0.1" -sS --max-time 5 \
        -o /dev/null -w 'HTTPS=%{http_code} TLS_VERIFY=%{ssl_verify_result}\n' "https://$domain/"; then
        verified=true
        break
    fi
    sleep .25
done
[[ $verified == true ]]
trap - ERR
echo 'HTTPS, TCP 6100 isolation, and automatic certificate renewal are enabled.'
echo 'A 502 response is expected until the designated ClipFlow host starts FRP.'
systemctl list-timers --all clipflow-cert-renew.timer --no-pager
