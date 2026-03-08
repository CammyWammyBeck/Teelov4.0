from __future__ import annotations

from pathlib import Path

import boto3
import structlog
from botocore.client import BaseClient

from teelo.config import get_settings

logger = structlog.get_logger(__name__)
S3_MODELS_PREFIX = "models/"


def get_s3_client() -> BaseClient:
    settings = get_settings()
    return boto3.client(
        "s3",
        aws_access_key_id=settings.aws_access_key_id or None,
        aws_secret_access_key=settings.aws_secret_access_key or None,
    )


def upload_model(local_path: str) -> tuple[str, str]:
    settings = get_settings()
    client = get_s3_client()

    model_path = Path(local_path)
    if not model_path.exists():
        raise FileNotFoundError(f"Model file not found: {model_path}")

    meta_path = Path(f"{local_path}_meta.json")
    if not meta_path.exists():
        raise FileNotFoundError(f"Model metadata file not found: {meta_path}")

    model_key = f"{S3_MODELS_PREFIX}{model_path.name}"
    meta_key = f"{S3_MODELS_PREFIX}{meta_path.name}"

    client.upload_file(str(model_path), settings.s3_model_bucket, model_key)
    client.upload_file(str(meta_path), settings.s3_model_bucket, meta_key)
    logger.info(
        "storage.model_uploaded",
        bucket=settings.s3_model_bucket,
        model_key=model_key,
        meta_key=meta_key,
    )
    return model_key, meta_key


def download_model(remote_name: str, local_dir: str = "models") -> tuple[str, str]:
    settings = get_settings()
    client = get_s3_client()

    model_name = Path(remote_name).name
    model_key = f"{S3_MODELS_PREFIX}{model_name}"
    meta_key = f"{model_key}_meta.json"

    destination = Path(local_dir)
    destination.mkdir(parents=True, exist_ok=True)
    model_path = destination / model_name
    meta_path = destination / f"{model_name}_meta.json"

    client.download_file(settings.s3_model_bucket, model_key, str(model_path))
    client.download_file(settings.s3_model_bucket, meta_key, str(meta_path))
    logger.info(
        "storage.model_downloaded",
        bucket=settings.s3_model_bucket,
        model_key=model_key,
        meta_key=meta_key,
        model_path=str(model_path),
        meta_path=str(meta_path),
    )
    return str(model_path), str(meta_path)


def list_models() -> list[str]:
    settings = get_settings()
    client = get_s3_client()
    paginator = client.get_paginator("list_objects_v2")

    model_names: list[str] = []
    for page in paginator.paginate(Bucket=settings.s3_model_bucket, Prefix=S3_MODELS_PREFIX):
        for item in page.get("Contents", []):
            key = item.get("Key", "")
            if not key or key.endswith("/") or key.endswith("_meta.json"):
                continue
            model_names.append(Path(key).name)

    models = sorted(model_names)
    logger.info("storage.models_listed", bucket=settings.s3_model_bucket, count=len(models))
    return models
