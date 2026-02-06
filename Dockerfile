FROM mcr.microsoft.com/playwright/python:v1.57.0-jammy

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Etc/UTC \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System packages for virtual display + VNC
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    xvfb \
    x11vnc \
    novnc \
    websockify \
 && rm -rf /var/lib/apt/lists/*

COPY requirements-all.txt /app/requirements-all.txt
COPY pyproject.toml /app/pyproject.toml
COPY src /app/src
COPY scripts /app/scripts
COPY alembic /app/alembic
COPY alembic.ini /app/alembic.ini

# Install deps (filter out the git editable line)
RUN python -m pip install --upgrade pip \
 && grep -v '^-e git' /app/requirements-all.txt > /tmp/requirements-docker.txt \
 && python -m pip install -r /tmp/requirements-docker.txt \
 && python -m pip install -e .

CMD ["python", "scripts/update_current_events.py"]
