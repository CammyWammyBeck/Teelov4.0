# Arch Server Setup (Headless Scrapers + Heroku DB)

This guide assumes the repo is cloned on the Arch server and you only want to
run scrapers (no API/web). VNC access is localhost-only with SSH tunneling.

## 1) One-time install

```bash
git clone <your_repo> /opt/teelo
cd /opt/teelo
./scripts/server_bootstrap_arch.sh
```

## 2) Configure environment

```bash
cp .env.example .env
$EDITOR .env
```

Required values:
- `DATABASE_URL` (Heroku, include `sslmode=require`)
- `SCRAPE_HEADLESS=false`
- `SCRAPE_VIRTUAL_DISPLAY=true`

Defaults already enforce localhost-only VNC:
- `SCRAPE_VNC_BIND=127.0.0.1`
- `SCRAPE_NOVNC_BIND=127.0.0.1`

If you want to access VNC from another computer without SSH:
- Set `SCRAPE_VNC_BIND=0.0.0.0`
- Set `SCRAPE_NOVNC_BIND=0.0.0.0`
- Set a password: `SCRAPE_VNC_PASSWORD=your_password`
- Ensure your firewall allows ports `5900` and/or `6080` only from trusted IPs

## 3) Migrations

```bash
source venv/bin/activate
alembic upgrade head
```

## 4) systemd units

```bash
sudo useradd -r -m -d /opt/teelo -s /bin/bash teelo || true
sudo chown -R teelo:teelo /opt/teelo

sudo cp docs/systemd/teelo-backfill.service /etc/systemd/system/
sudo cp docs/systemd/teelo-update.service /etc/systemd/system/
sudo cp docs/systemd/teelo-update.timer /etc/systemd/system/

sudo systemctl daemon-reload
```

If your repo path is not `/opt/teelo`, update the service files to match:
- `WorkingDirectory`
- `EnvironmentFile`
- `ExecStart`
- `User` / `Group`

## 5) Run backfill

```bash
sudo systemctl start teelo-backfill
journalctl -u teelo-backfill -f
```

## 6) Enable regular updates

```bash
sudo systemctl enable --now teelo-update.timer
systemctl list-timers teelo-update.timer
```

## 7) View browser

Option A: SSH tunnel (more secure)

```bash
ssh -L 6080:localhost:6080 -L 5900:localhost:5900 user@your-server
```

Then open:
```
http://localhost:6080/vnc.html
```

Option B: Direct IP access

Set `SCRAPE_VNC_BIND=0.0.0.0` and `SCRAPE_NOVNC_BIND=0.0.0.0` in `.env`.

Then open:
```
http://<server-ip>:6080/vnc.html
```

## 8) Update deployment later

```bash
cd /opt/teelo
./scripts/deploy_update.sh
```
