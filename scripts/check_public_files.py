"""Check files eligible for Git publication without printing sensitive values."""
from __future__ import annotations
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IGNORED_DIRS = {".git", ".venv", "venv", "__pycache__", "data", "output", "rendered_sql", "credentials", ".vscode", ".idea"}


def public_files() -> list[Path]:
    probe = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "--show-toplevel"], capture_output=True, text=True)
    if probe.returncode == 0 and Path(probe.stdout.strip()).resolve() == ROOT.resolve():
        result = subprocess.run(["git", "-C", str(ROOT), "ls-files", "-z", "--cached", "--others", "--exclude-standard"], capture_output=True, check=True)
        names = set(result.stdout.decode("utf-8").split("\0")) - {""}
        return sorted(ROOT / name for name in names if (ROOT / name).is_file())
    return sorted(p for p in ROOT.rglob("*") if p.is_file() and not set(p.relative_to(ROOT).parts) & IGNORED_DIRS
                  and not (p.parent.name == "config" and ".example." not in p.name and not p.name.endswith(".env.example")))


def issues(relative: Path, text: str) -> list[str]:
    result = []
    if set(relative.parts) & IGNORED_DIRS:
        result.append("private or generated directory")
    if relative.parts[0] == "config" and ".example." not in relative.name and not relative.name.endswith(".env.example"):
        result.append("actual configuration file")
    if relative.name.startswith(".env") and relative.name != ".env.example":
        result.append("actual environment file")
    if relative.suffix in {".parquet", ".sqlite", ".xlsx", ".key", ".pem", ".log", ".stackdump", ".pyc"}:
        result.append("data, credential or temporary artifact")
    patterns = (
        r"AIza[0-9A-Za-z_-]{30,}", r"\bya29\.[0-9A-Za-z_.-]{20,}",
        r"\bEA[A-Z][0-9A-Za-z]{50,}", r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----",
        r"\bgh[pousr]_[A-Za-z0-9]{30,}",
    )
    if any(re.search(pattern, text) for pattern in patterns):
        result.append("possible embedded credential")
    # Direct assignment of a credential to a literal; placeholders are allowed.
    for match in re.finditer(r"(?:api_key|access_token|app_secret|client_secret|password|refresh_token)\s*[:=]\s*['\"]([^'\"\n]+)['\"]", text, re.I):
        value = match.group(1)
        if len(value) > 8 and not value.startswith(("REPLACE_", "YOUR_")):
            result.append("possible credential literal")
            break
    return result


def validate_files(files: list[Path]) -> list[str]:
    findings = []
    for path in files:
        relative = path.relative_to(ROOT)
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            findings.append(f"{relative.as_posix()}: unexpected binary file")
            continue
        findings.extend(f"{relative.as_posix()}: {issue}" for issue in issues(relative, text))
    return findings


def main():
    files = public_files()
    findings = validate_files(files)
    if findings:
        print("\n".join(findings))
        raise SystemExit(1)
    print(f"Public-file check passed: {len(files)} files. No credential patterns or private artifacts found.")


if __name__ == "__main__":
    main()
