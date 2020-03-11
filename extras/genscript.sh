#!/bin/bash

# genscript.sh is used at build stage, it accepts target building environment and produces install.sh script for agent
# distribution. Different environments uses different FQDN's and this script create script with appropriate FQDNs.

[[ $# == 0 ]] && { echo "environment is not specified, exit"; exit 1; }
ENV=$1
case $ENV in
  development)
    BASE_DOMAIN=127.0.0.1
    ;;
  staging)
    BASE_DOMAIN=wpnr.brcd.pro
    ;;
  production)
    BASE_DOMAIN=weaponry.io
    ;;
  *)
    echo 'unknown environment, exit'
    exit 1
    ;;
esac

cat <<EOF
#!/bin/bash

# sanity checks
if [ \$# -eq 0 ]; then { echo "API key is not specified, exit"; exit 1; }; fi

API_KEY=\$1

# download and extract agent
curl -s https://dist.${BASE_DOMAIN}/weaponry-agent.tar.gz -o - | tar xzf -

# run agent bootstrap using passed key
./weaponry-agent --bootstrap --api-key=\${API_KEY} --metric-service-url=https://push.${BASE_DOMAIN}
EOF