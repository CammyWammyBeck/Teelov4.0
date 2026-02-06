# Docker Setup (Recommended)

This path avoids host Python version issues and makes updates easy.

## 0) Clean up the old host setup (optional)

If you already started a host-based setup:

```bash
cd ~/Teelov4.0
rm -rf venv
```

## 1) Install Docker

```bash
sudo pacman -S --needed docker docker-compose
sudo systemctl enable --now docker
sudo usermod -aG docker cammybeck
```

Log out and back in so the group change applies.

## 2) Build the image

```bash
cd ~/Teelov4.0
docker compose build
```

## 3) Create `.env`

```bash
cp .env.example .env
```

Fill in:
```
DATABASE_URL=postgresql://...your_heroku... ?sslmode=require
SCRAPE_HEADLESS=false
SCRAPE_VIRTUAL_DISPLAY=true
SCRAPE_VNC_BIND=0.0.0.0
SCRAPE_NOVNC_BIND=0.0.0.0
SCRAPE_VNC_PASSWORD=your_password
```

## 4) Run migrations

```bash
docker compose run --rm teelo-update alembic upgrade head
```

## 5) Run backfill (one-time)

```bash
docker compose run --rm teelo-backfill
```

## 6) Run current updates manually

```bash
docker compose run --rm teelo-update
```

## 7) View VNC in browser

```
http://<server-ip>:6080/vnc.html
```

## 8) Update later

```bash
cd ~/Teelov4.0
git pull
docker compose build
```

Then run:
```bash
docker compose run --rm teelo-update
```

## 9) Scheduling (optional)

Use cron on the host to run updates on a schedule:

```bash
crontab -e
```

Example: hourly
```
0 * * * * cd /home/cammybeck/Teelov4.0 && docker compose run --rm teelo-update >> /home/cammybeck/teelo-update.log 2>&1
```
