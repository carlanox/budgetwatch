# BudgetWatch — Hostinger VPS Deployment

Deploys the BudgetWatch backend to a Hostinger VPS that already runs n8n + Traefik + gowa.

## What's in this package

```
deploy/
├── docker-compose.yml         # Stack: API + Postgres + Cron, joins n8n_default network
├── setup.sh                   # One-shot setup script
├── README.md                  # This file
└── budgetwatch/               # Backend Python code + Dockerfile
    ├── Dockerfile
    ├── requirements.txt
    ├── api.py                 # FastAPI server
    ├── connectors.py          # DJPK, INAPROC, SPSE Nasional, e-Katalog
    ├── connectors_jakarta.py  # data.jakarta.go.id (CKAN), BPKD dashboard
    ├── fixtures.py            # 30-item demo data
    ├── marketplace_scrapers.py # Tokopedia, Shopee, e-Katalog scrapers
    ├── matching.py            # 20% flag rule + spec matcher
    ├── models.py              # LineItem schema + classifier
    ├── search_backends.py     # In-memory search (Meilisearch swap-in later)
    └── store_pg.py            # Postgres-backed storage layer
```

## Prerequisites — already done on your VPS

- ✅ Ubuntu 24.04 with n8n template
- ✅ Docker + Docker Compose
- ✅ Traefik (`n8n-traefik-1`) listening on 80/443 in `n8n_default` network
- ✅ DNS A record: `api.sonducts.com` → `72.61.143.211` (add this at rumahhosting before deploying)

## Step-by-step deploy

### 1. Upload the deploy folder to your VPS

From your laptop (or via Hostinger's file manager):

```bash
# From your local machine
scp -r ./deploy root@72.61.143.211:/opt/budgetwatch
```

Or use Hostinger File Manager → upload `deploy.zip` → unzip into `/opt/budgetwatch`.

### 2. SSH into your VPS

Via Hostinger Browser Terminal or:

```bash
ssh root@72.61.143.211
```

### 3. Verify DNS is pointing to your VPS

```bash
dig +short api.sonducts.com
# Should print: 72.61.143.211
```

If empty or wrong, wait 10–30 min for DNS propagation. Don't proceed until this works.

### 4. Run the setup script

```bash
cd /opt/budgetwatch
bash setup.sh
```

The script will:
- Generate strong random secrets in `.env`
- Build the API Docker image
- Bring up Postgres + API + cron containers
- Wait for the API to become healthy
- Print final test instructions

**Save the ADMIN_TOKEN** that the script prints — you'll need it for ingestion.

### 5. Verify HTTPS works (Traefik auto-provisions Let's Encrypt cert)

```bash
curl https://api.sonducts.com/api/v1/health
```

Should return:
```json
{"ok": true, "items_loaded": 0}
```

If you get a TLS error, your existing Traefik may use a different certresolver name. Check:

```bash
docker inspect n8n-traefik-1 | grep -A 1 -i certresolver
```

If the resolver name isn't `letsencrypt`, edit `docker-compose.yml`:

```yaml
- "traefik.http.routers.budgetwatch.tls.certresolver=YOUR_RESOLVER_NAME"
```

Then `docker compose up -d` again.

### 6. Trigger first ingestion

```bash
source /opt/budgetwatch/.env
curl -X POST https://api.sonducts.com/admin/ingest/dki-jakarta \
     -H "Authorization: Bearer $ADMIN_TOKEN"
```

This pulls live data from data.jakarta.go.id (CKAN), BPKD dashboard, SPSE Nasional, and DJPK.
First run takes ~30–60 seconds. Returns a JSON summary of items ingested per source.

### 7. Verify the data is live

```bash
curl https://api.sonducts.com/api/v1/provinces/dki-jakarta
curl https://api.sonducts.com/api/v1/provinces/dki-jakarta/agencies
```

You should now see real Jakarta APBD data, not the 30 demo items.

## Frontend deploy (Vercel)

1. Push the BudgetWatchJakarta.jsx (the artifact you've been building) to a GitHub repo as a Next.js project
2. Deploy to Vercel (sign in with GitHub, "Import Project")
3. In the JSX, change `API_BASE` from `http://127.0.0.1:8765/api/v1` to `https://api.sonducts.com/api/v1`
4. Set Vercel project's custom domain to `budgetwatch.sonducts.com`
5. Add another DNS record at rumahhosting:

| Type  | Name        | Value                  |
|-------|-------------|------------------------|
| CNAME | budgetwatch | cname.vercel-dns.com   |

Vercel handles HTTPS automatically.

## Daily operations

### Manually trigger ingestion

```bash
source /opt/budgetwatch/.env
curl -X POST https://api.sonducts.com/admin/ingest/dki-jakarta \
     -H "Authorization: Bearer $ADMIN_TOKEN"
```

### View logs

```bash
docker compose logs -f budgetwatch-api
docker compose logs -f budgetwatch-cron
```

### Stop / start

```bash
docker compose stop      # Stop without removing
docker compose down      # Remove containers (keeps volumes/data)
docker compose up -d     # Start again
```

### Update the code

```bash
cd /opt/budgetwatch
# Pull/copy new files into /opt/budgetwatch/budgetwatch/
docker compose build budgetwatch-api
docker compose up -d budgetwatch-api
```

### Backup the database

```bash
docker exec budgetwatch-db pg_dump -U budgetwatch budgetwatch \
    | gzip > /root/budgetwatch-$(date +%Y%m%d).sql.gz
```

## Resource expectations

For DKI Jakarta with full data (~150k APBD records):

- Postgres disk: ~500 MB
- API memory: ~200 MB resident
- Cron: negligible
- Total CPU: < 5% on idle, ~50% during ingestion (60 seconds, daily)

Your KVM 1 plan (1 vCPU / 4 GB RAM) is sufficient. If you expand to all 38
provinces, consider upgrading to KVM 2.

## Troubleshooting

**Cert not provisioning:** Traefik certresolver name may be different. Check
with `docker inspect n8n-traefik-1 | grep -i certresolver` and update
docker-compose.yml accordingly.

**API can't reach data.jakarta.go.id:** Confirm outbound HTTPS is allowed:
`docker exec budgetwatch-api curl -I https://data.jakarta.go.id`

**Database connection error:** Check `docker compose logs budgetwatch-db` —
the API waits for healthcheck but might race on slow VPS.

**Ingestion returns errors per source:** That's expected — some endpoints
may be temporarily down. Other sources still complete. Check the response
JSON `errors` field for details.
