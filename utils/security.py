import base64
import hashlib
import os

import streamlit as st
from cryptography.fernet import Fernet


DEFAULT_SECRET_SEED = "fortress-dashboard-dev-secret-change-me"


def _get_secret_seed() -> str:
    if os.getenv("FORTRESS_ENCRYPTION_KEY"):
        return os.environ["FORTRESS_ENCRYPTION_KEY"]
    try:
        return st.secrets["fortress"]["encryption_key"]
    except Exception:
        return DEFAULT_SECRET_SEED


def _derive_fernet_key(seed: str) -> bytes:
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


def get_fernet() -> Fernet:
    return Fernet(_derive_fernet_key(_get_secret_seed()))


def encrypt_token(token: str) -> str:
    if not token:
        return ""
    return get_fernet().encrypt(token.encode("utf-8")).decode("utf-8")


def decrypt_token(token_encrypted: str) -> str:
    if not token_encrypted:
        return ""
    return get_fernet().decrypt(token_encrypted.encode("utf-8")).decode("utf-8")


def hash_password(password: str) -> str:
    """Simple SHA-256 hashing for user passwords."""
    if not password:
        return ""
    return hashlib.sha256(password.encode("utf-8")).hexdigest()
