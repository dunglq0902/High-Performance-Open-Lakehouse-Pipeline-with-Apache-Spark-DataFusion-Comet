"""Create local synthetic credentials without printing or replacing them."""

from __future__ import annotations

import argparse
import os
import secrets
from pathlib import Path


def ensure_environment(path: Path) -> str:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    declared: dict[str, str] = {}
    for line_number, line in enumerate(existing.splitlines(), 1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        name, separator, value = line.partition("=")
        name = name.strip()
        if not separator or not name:
            raise ValueError(f"invalid environment entry at {path}:{line_number}")
        if name in declared:
            raise ValueError(f"duplicate environment key at {path}:{line_number}: {name}")
        if value.startswith("replace-with-"):
            raise ValueError(f"placeholder value is not allowed in {path}: {name}")
        declared[name] = value
    generated = {
        "MINIO_ROOT_USER": f"lh{secrets.token_hex(8)}",
        "MINIO_ROOT_PASSWORD": secrets.token_urlsafe(32),
        "ICEBERG_JDBC_PASSWORD": secrets.token_urlsafe(24),
        "AWS_REGION": "us-east-1",
    }
    missing = [name for name in generated if name not in declared]
    if not missing:
        if os.name != "nt":
            path.chmod(0o600)
        return "kept"
    prefix = existing
    if prefix and not prefix.endswith("\n"):
        prefix += "\n"
    payload = prefix + "".join(f"{name}={generated[name]}\n" for name in missing)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload, encoding="utf-8", newline="\n")
    if os.name != "nt":
        path.chmod(0o600)
    return "updated" if existing else "created"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path(".env"))
    args = parser.parse_args()
    state = ensure_environment(args.output)
    print(f"{state} {args.output}")


if __name__ == "__main__":
    main()
