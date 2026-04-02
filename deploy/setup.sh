#!/bin/bash
# ABS Dashboard — Hostinger VPS initial setup
# Run as root: bash setup.sh
# Then set your ANTHROPIC_API_KEY in /opt/abs_dashboard/.env

set -e

APP_DIR="/opt/abs_dashboard"
SERVICE_USER="absapp"
REPO="https://github.com/bchamlet/abs_dashboard.git"

echo "=== 1. System update ==="
apt-get update && apt-get upgrade -y

echo "=== 2. Install dependencies ==="
apt-get install -y python3 python3-pip python3-venv git nginx certbot python3-certbot-nginx

echo "=== 3. Create app user ==="
id -u $SERVICE_USER &>/dev/null || useradd -m -s /bin/bash $SERVICE_USER

echo "=== 4. Clone repository ==="
mkdir -p $APP_DIR
git clone $REPO $APP_DIR || (cd $APP_DIR && git pull)
chown -R $SERVICE_USER:$SERVICE_USER $APP_DIR

echo "=== 5. Python virtual environment ==="
sudo -u $SERVICE_USER python3 -m venv $APP_DIR/venv
sudo -u $SERVICE_USER $APP_DIR/venv/bin/pip install --upgrade pip
sudo -u $SERVICE_USER $APP_DIR/venv/bin/pip install -r $APP_DIR/requirements.txt

echo "=== 6. Create data directory ==="
mkdir -p $APP_DIR/data
chown -R $SERVICE_USER:$SERVICE_USER $APP_DIR/data

echo "=== 7. Create .env file ==="
if [ ! -f $APP_DIR/.env ]; then
    cat > $APP_DIR/.env <<EOF
ANTHROPIC_API_KEY=your_api_key_here
EOF
    chown $SERVICE_USER:$SERVICE_USER $APP_DIR/.env
    chmod 600 $APP_DIR/.env
    echo "  --> Edit $APP_DIR/.env and set your ANTHROPIC_API_KEY"
fi

echo "=== 8. Install systemd service ==="
cp $APP_DIR/deploy/abs-dashboard.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable abs-dashboard

echo "=== 9. Configure nginx ==="
cp $APP_DIR/deploy/nginx.conf /etc/nginx/sites-available/abs-dashboard
ln -sf /etc/nginx/sites-available/abs-dashboard /etc/nginx/sites-enabled/abs-dashboard
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl reload nginx

echo ""
echo "======================================================"
echo " Setup complete. Next steps:"
echo "======================================================"
echo " 1. Edit /opt/abs_dashboard/.env — set ANTHROPIC_API_KEY"
echo " 2. Edit /etc/nginx/sites-available/abs-dashboard"
echo "      — replace YOUR_DOMAIN_OR_IP with your domain or IP"
echo " 3. systemctl start abs-dashboard"
echo " 4. systemctl status abs-dashboard"
echo ""
echo " Optional — free SSL certificate:"
echo "   certbot --nginx -d your.domain.com"
echo "======================================================"
