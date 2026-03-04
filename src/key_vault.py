"""
key_vault.py — Zero-knowledge private key storage.

How it works:
  - User sets a personal PIN with /setpin <PIN>
  - When /setkey is called, the key is encrypted with AES-256-GCM using a
    key derived from their PIN via PBKDF2-HMAC-SHA256 (310,000 iterations)
  - Only the encrypted ciphertext + salt + nonce are written to disk
  - The server owner CANNOT decrypt the key without the user's PIN
  - Decrypted key lives in memory only for the active session
  - On /lock or session restart, plaintext is wiped from memory

Storage format (per user, in encrypted_keys.json):
  {
    "<user_id>": {
      "salt": "<hex>",
      "nonce": "<hex>",
      "ciphertext": "<hex>",
      "tag": "<hex>"
    }
  }

The plaintext private key is NEVER written to disk.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# PBKDF2 parameters
_ITERATIONS = 310_000
_KEY_LEN = 32   # AES-256
_SALT_LEN = 32
_NONCE_LEN = 12


def _derive_key(pin: str, salt: bytes, user_id: int) -> bytes:
    """Derive a 256-bit AES key from PIN + salt + user_id (as extra entropy)."""
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.hazmat.primitives import hashes

    # Mix user_id into the salt so even identical PINs produce different keys
    combined_salt = salt + str(user_id).encode()
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=_KEY_LEN,
        salt=combined_salt,
        iterations=_ITERATIONS,
    )
    return kdf.derive(pin.encode("utf-8"))


def encrypt_key(plaintext_key: str, pin: str, user_id: int) -> dict:
    """Encrypt a private key with the user's PIN. Returns a storable dict."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    salt = secrets.token_bytes(_SALT_LEN)
    nonce = secrets.token_bytes(_NONCE_LEN)
    aes_key = _derive_key(pin, salt, user_id)

    aesgcm = AESGCM(aes_key)
    ciphertext_with_tag = aesgcm.encrypt(nonce, plaintext_key.encode("utf-8"), None)

    # AESGCM appends 16-byte tag to ciphertext
    ciphertext = ciphertext_with_tag[:-16]
    tag = ciphertext_with_tag[-16:]

    return {
        "salt":       salt.hex(),
        "nonce":      nonce.hex(),
        "ciphertext": ciphertext.hex(),
        "tag":        tag.hex(),
    }


def decrypt_key(vault_entry: dict, pin: str, user_id: int) -> Optional[str]:
    """
    Decrypt a vault entry with the user's PIN.
    Returns plaintext key string, or None if PIN is wrong.
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.exceptions import InvalidTag

    try:
        salt = bytes.fromhex(vault_entry["salt"])
        nonce = bytes.fromhex(vault_entry["nonce"])
        ciphertext = bytes.fromhex(vault_entry["ciphertext"])
        tag = bytes.fromhex(vault_entry["tag"])

        aes_key = _derive_key(pin, salt, user_id)
        aesgcm = AESGCM(aes_key)

        plaintext = aesgcm.decrypt(nonce, ciphertext + tag, None)
        return plaintext.decode("utf-8")
    except (InvalidTag, KeyError, ValueError):
        return None  # Wrong PIN or corrupted entry


class KeyVault:
    """
    Manages encrypted key storage and in-memory session decryption.

    - Encrypted blobs are persisted to disk.
    - Plaintext keys live ONLY in self._session_keys (in-memory dict).
    - Call lock(user_id) or lock_all() to wipe plaintext from memory.
    """

    def __init__(self, store_path: str = ".data/encrypted_keys.json") -> None:
        self._store_path = store_path
        self._vault: dict[str, dict] = self._load()
        # In-memory only — never persisted
        self._session_keys: dict[int, str] = {}
        # Hashed PINs for re-auth (we store hash, not PIN itself)
        self._pin_hashes: dict[int, str] = {}

    def _load(self) -> dict[str, dict]:
        try:
            with open(self._store_path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except FileNotFoundError:
            return {}
        except Exception as exc:
            logger.error("KeyVault load error: %s", exc)
            return {}

    def _save(self) -> None:
        try:
            Path(self._store_path).parent.mkdir(parents=True, exist_ok=True)
            with open(self._store_path, "w", encoding="utf-8") as fh:
                json.dump(self._vault, fh, indent=2)
        except Exception as exc:
            logger.error("KeyVault save error: %s", exc)

    # ── PIN management ─────────────────────────────────────────────────────────

    def set_pin(self, user_id: int, pin: str) -> bool:
        """Store a hashed PIN for the user. Returns False if PIN is too weak."""
        if len(pin) < 4:
            return False
        pin_hash = hashlib.sha256(
            (pin + str(user_id) + "btcpred_salt").encode()
        ).hexdigest()
        self._pin_hashes[user_id] = pin_hash
        return True

    def has_pin(self, user_id: int) -> bool:
        return user_id in self._pin_hashes

    def verify_pin(self, user_id: int, pin: str) -> bool:
        expected = self._pin_hashes.get(user_id)
        if not expected:
            return False
        pin_hash = hashlib.sha256(
            (pin + str(user_id) + "btcpred_salt").encode()
        ).hexdigest()
        return secrets.compare_digest(pin_hash, expected)

    # ── Key storage ────────────────────────────────────────────────────────────

    def store_key(self, user_id: int, plaintext_key: str, pin: str) -> bool:
        """
        Encrypt and persist the user's private key.
        Also caches the plaintext in session memory so trading can start immediately.
        Returns False if PIN verification fails.
        """
        if not self.verify_pin(user_id, pin):
            return False
        entry = encrypt_key(plaintext_key, pin, user_id)
        self._vault[str(user_id)] = entry
        self._save()
        # Cache in session (wiped on lock/restart)
        self._session_keys[user_id] = plaintext_key
        return True

    def has_encrypted_key(self, user_id: int) -> bool:
        return str(user_id) in self._vault

    def unlock(self, user_id: int, pin: str) -> bool:
        """
        Decrypt the stored key into session memory using the user's PIN.
        Returns True on success.
        """
        entry = self._vault.get(str(user_id))
        if not entry:
            return False
        plaintext = decrypt_key(entry, pin, user_id)
        if plaintext is None:
            return False
        self._session_keys[user_id] = plaintext
        return True

    def get_session_key(self, user_id: int) -> Optional[str]:
        """Return the in-memory decrypted key, or None if locked."""
        return self._session_keys.get(user_id)

    def is_unlocked(self, user_id: int) -> bool:
        return user_id in self._session_keys

    def lock(self, user_id: int) -> None:
        """Wipe the plaintext key from memory. Encrypted blob stays on disk."""
        self._session_keys.pop(user_id, None)

    def lock_all(self) -> None:
        """Wipe all session keys from memory."""
        self._session_keys.clear()

    def delete_key(self, user_id: int) -> None:
        """Permanently delete a user's encrypted key from disk and memory."""
        self._vault.pop(str(user_id), None)
        self._session_keys.pop(user_id, None)
        self._pin_hashes.pop(user_id, None)
        self._save()

    def vault_status(self, user_id: int) -> str:
        has_vault = self.has_encrypted_key(user_id)
        unlocked = self.is_unlocked(user_id)
        has_pin = self.has_pin(user_id)
        if not has_pin:
            return "No PIN set. Use /setpin <PIN> first."
        if not has_vault:
            return "PIN set. No key stored yet. Use /setkey <private_key>."
        if unlocked:
            return "🔓 Unlocked — key active in session memory."
        return "🔒 Locked — use /unlock <PIN> to activate trading."


# Singleton
key_vault = KeyVault()
