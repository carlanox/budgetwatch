#!/bin/bash
# BudgetWatch — VPS Setup Script
#
# Run this ONCE on your Hostinger VPS as root, after uploading the deploy folder.
#
#   cd /opt/budgetwatch
#   bash setup.sh
#
# What it does:
#   1. Verifies Docker + the n8n_default network are present
#   2. Generates a strong .env if you don't have one yet
#   3. Checks Traefik's Let's Encrypt configuration
#   4. Brings up the BudgetWatch stack
#   5. Triggers the first ingestion run

set -euo pipefail

# ---- Colors for readability ----
G='\033[0;32m'; Y='\033[0;33m'; R='\033[0;31m'; B='\033[0;34m'; N='\033[0m'
info()  { echo -e "${B}[INFO]${N} $*"; }
ok()    { echo -e "${G}[ OK ]${N} $*"; }
warn()  { echo -e "${Y}[WARN]${N} $*"; }
fail()  { echo -e "${R}[FAIL]${N} $*"; exit 1; }

# ---- Pre-flight checks ----
info "Checking prerequisites..."
command -v docker >/dev/null || fail "Docker not found"
docker compose version >/dev/null 2>&1 || fail "Docker Compose v2 not found"
docker network inspect n8n_default >/dev/null 2>&1 || fail "n8n_default network not found"
ok "Docker + n8n_default network present"

# ---- Check Traefik's Let's Encrypt setup ----
info "Inspecting existing Traefik configuration..."
TRAEFIK_CMD=$(docker inspect n8n-traefik-1 --format '{{join .Args " "}}' 2>/dev/null || echo "")
if echo "$TRAEFIK_CMD" | grep -q "letsencrypt"; then
    ok "Traefik already has 'letsencrypt' certresolver configured"
else
    warn "Traefik may not have a 'letsencrypt' certresolver named exactly 'letsencrypt'."
    warn "Inspecting Traefik command line..."
    echo "$TRAEFIK_CMD" | tr ' ' '\n' | grep -E '(certresolver|acme|letsencrypt)' || \
        warn "No certresolver flags found. You may need to use the existing one or set up SSL manually."
    warn "If our deploy fails on cert provisioning, edit docker-compose.yml and change"
    warn "  'traefik.http.routers.budgetwatch.tls.certresolver=letsencrypt'"
    warn "to match your existing Traefik certresolver name."
fi

# ---- Generate .env if missing ----
if [ ! -f .env ]; then
    info "Generating .env with random secrets..."
    cat > .env <<EOF
# BudgetWatch environment — DO NOT COMMIT TO GIT
POSTGRES_PASSWORD=$(openssl rand -hex 32)
ADMIN_TOKEN=$(openssl rand -hex 32)
FRONTEND_ORIGIN=https://budgetwatch.sonducts.com
EOF
    chmod 600 .env
    ok ".env created with random POSTGRES_PASSWORD + ADMIN_TOKEN"
    info "Your admin token (save this — you'll need it for manual ingestion):"
    grep ADMIN_TOKEN .env
else
    ok ".env already exists, keeping existing secrets"
fi

# ---- Build and start ----
info "Building budgetwatch-api image..."
docker compose build budgetwatch-api

info "Starting stack..."
docker compose up -d

# ---- Wait for health ----
info "Waiting for API to be healthy (max 60s)..."
for i in {1..30}; do
    if docker exec budgetwatch-api curl -sf http://localhost:8000/api/v1/health >/dev/null 2>&1; then
        ok "API is responding"
        break
    fi
    sleep 2
    [ $i -eq 30 ] && fail "API did not become healthy within 60s. Check: docker compose logs budgetwatch-api"
done

# ---- Show DNS instructions ----
echo ""
echo "==================================================================="
echo "  Stack is running locally. Final steps:"
echo "==================================================================="
echo ""
echo "1. Verify DNS:"
echo "     dig +short api.sonducts.com"
echo "   Should return: 72.61.143.211"
echo ""
echo "2. After DNS propagates, test the public endpoint:"
echo "     curl https://api.sonducts.com/api/v1/health"
echo ""
echo "3. Trigger first ingestion:"
echo "     source .env"
echo "     curl -X POST https://api.sonducts.com/admin/ingest/dki-jakarta \\"
echo "          -H \"Authorization: Bearer \$ADMIN_TOKEN\""
echo ""
echo "4. View logs:"
echo "     docker compose logs -f budgetwatch-api"
echo ""
echo "==================================================================="
ok "Setup complete."
