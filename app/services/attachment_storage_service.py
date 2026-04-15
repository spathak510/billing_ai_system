from __future__ import annotations

from pathlib import Path


class AttachmentStorageService:
    """Reusable helper to store attachments with extension filtering and safe unique naming."""

    def __init__(
        self,
        storage_dir: str | Path,
        allowed_extensions: set[str],
    ) -> None:
        self._default_storage_dir = Path(storage_dir)
        self._allowed_extensions = {ext.lower() for ext in allowed_extensions}

    def save_if_allowed(
        self,
        raw_name: str,
        content: bytes,
        storage_dir: str | Path | None = None,
    ) -> str | None:
        """Save attachment content if extension is allowed; otherwise return None."""
        safe_name = Path(raw_name or "attachment.bin").name
        if Path(safe_name).suffix.lower() not in self._allowed_extensions:
            return None

        target_dir = Path(storage_dir) if storage_dir is not None else self._default_storage_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = self._next_available_path(target_dir / safe_name)
        target_path.write_bytes(content)
        return str(target_path)

    @staticmethod
    def _next_available_path(path: Path) -> Path:
        if not path.exists():
            return path

        stem = path.stem
        suffix = path.suffix
        index = 1
        while True:
            candidate = path.parent / f"{stem}_{index}{suffix}"
            if not candidate.exists():
                return candidate
            index += 1
