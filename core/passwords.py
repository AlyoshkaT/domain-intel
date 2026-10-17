"""
Password hashing — bcrypt with a safe plaintext fallback.

Degrade-safe by design: if bcrypt is unavailable for any reason, hash_password
returns the plaintext unchanged and verify_password compares plaintext — so auth
keeps working exactly as before and a bcrypt problem can never lock anyone out.
Once bcrypt is present, new/updated passwords are stored as bcrypt hashes, and
legacy plaintext rows migrate to a hash on the next successful login
(see api/auth.py). bcrypt hashes and plaintext coexist indefinitely.
"""
import logging

logger = logging.getLogger(__name__)


def is_hash(s) -> bool:
    """True if the stored value is already a bcrypt hash."""
    return isinstance(s, str) and s.startswith(("$2a$", "$2b$", "$2y$"))


def hash_password(plain: str) -> str:
    """Return a bcrypt hash of `plain`. Idempotent (an already-hashed value is
    returned unchanged). Falls back to returning `plain` if bcrypt is unavailable."""
    if not plain or is_hash(plain):
        return plain
    try:
        import bcrypt
        # bcrypt hard-caps the password at 72 bytes; truncate explicitly so very
        # long inputs hash deterministically instead of raising on newer bcrypt.
        return bcrypt.hashpw(plain.encode("utf-8")[:72], bcrypt.gensalt()).decode("utf-8")
    except Exception as e:
        logger.warning(f"bcrypt unavailable — storing password as plaintext: {e}")
        return plain


def verify_password(stored: str, provided: str) -> bool:
    """Check `provided` against the `stored` value (bcrypt hash or legacy plaintext)."""
    if not stored or provided is None:
        return False
    if is_hash(stored):
        try:
            import bcrypt
            return bcrypt.checkpw(provided.encode("utf-8")[:72], stored.encode("utf-8"))
        except Exception as e:
            logger.error(f"bcrypt verify failed: {e}")
            return False
    return provided == stored  # legacy plaintext
