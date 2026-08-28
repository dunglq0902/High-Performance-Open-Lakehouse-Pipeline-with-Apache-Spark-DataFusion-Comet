from pathlib import Path

from scripts.redact_logs import redact_file, redact_text, sensitive_values


def test_sensitive_values_and_text_redaction(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "MINIO_ROOT_USER=local-user\n"
        "MINIO_ROOT_PASSWORD=long-local-password\n"
        "AWS_REGION=us-east-1\n",
        encoding="utf-8",
    )
    values = sensitive_values(env_file)
    assert values == ("long-local-password", "local-user")
    assert redact_text("local-user:long-local-password@us-east-1", values) == (
        "<redacted>:<redacted>@us-east-1"
    )


def test_redact_file_replaces_secret_without_touching_safe_log(tmp_path: Path) -> None:
    secret_log = tmp_path / "secret.log"
    secret_log.write_text("password=do-not-keep\n", encoding="utf-8")
    safe_log = tmp_path / "safe.log"
    safe_log.write_text("all good\n", encoding="utf-8")

    assert redact_file(secret_log, ("do-not-keep",))
    assert secret_log.read_text(encoding="utf-8") == "password=<redacted>\n"
    assert not redact_file(safe_log, ("do-not-keep",))
