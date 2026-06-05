"""Download a portable Temurin (Eclipse Adoptium) JDK 17 into ``.jdk/``.

PySpark 4.1 needs a Java 17+ runtime. Many machines don't have one and can't
``apt install`` without sudo. This script downloads a self-contained JDK tarball
from the Adoptium API and unpacks it into the project's ``.jdk/`` directory
(git-ignored). After running it, :func:`utils.spark.build_session` will find the
JDK automatically.

Usage::

    uv run python scripts/bootstrap_java.py

Idempotent: if a JDK is already present in ``.jdk/`` it does nothing.
"""

from __future__ import annotations

import platform
import shutil
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

JDK_FEATURE_VERSION = 17
PROJECT_ROOT = Path(__file__).resolve().parent.parent
JDK_DIR = PROJECT_ROOT / ".jdk"

# Map Python's platform names to Adoptium API values.
_ARCH_MAP = {
    "x86_64": "x64",
    "amd64": "x64",
    "aarch64": "aarch64",
    "arm64": "aarch64",
}
_OS_MAP = {
    "linux": "linux",
    "darwin": "mac",
}


def _adoptium_url() -> str:
    machine = platform.machine().lower()
    system = platform.system().lower()
    arch = _ARCH_MAP.get(machine)
    os_name = _OS_MAP.get(system)
    if arch is None or os_name is None:
        raise SystemExit(
            f"Unsupported platform: system={system!r} machine={machine!r}. "
            "Install a JDK 17+ manually and set JAVA_HOME."
        )
    # Adoptium "latest" redirect — always serves the newest GA build.
    return (
        f"https://api.adoptium.net/v3/binary/latest/{JDK_FEATURE_VERSION}/ga/"
        f"{os_name}/{arch}/jdk/hotspot/normal/eclipse?project=jdk"
    )


def find_existing_jdk() -> Path | None:
    """Return the home of an already-extracted JDK in ``.jdk/``, if any."""
    if not JDK_DIR.exists():
        return None
    for java in JDK_DIR.rglob("bin/java"):
        # On macOS the home is the dir containing bin/; Contents/Home on mac
        # tarballs is handled the same way since we look for bin/java directly.
        return java.parent.parent
    return None


def bootstrap() -> Path:
    existing = find_existing_jdk()
    if existing is not None:
        print(f"JDK already present at {existing}")
        return existing

    JDK_DIR.mkdir(parents=True, exist_ok=True)
    url = _adoptium_url()
    print(f"Downloading Temurin JDK {JDK_FEATURE_VERSION} from:\n  {url}")

    # Adoptium redirects to GitHub release assets, which reject the default
    # "Python-urllib" User-Agent with 403. Send a normal UA.
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        with urllib.request.urlopen(req) as resp:  # noqa: S310 (trusted host)
            shutil.copyfileobj(resp, tmp)

    print("Extracting...")
    with tarfile.open(tmp_path, "r:gz") as tar:
        members = tar.getmembers()
        tar.extractall(JDK_DIR, filter="data")
    tmp_path.unlink(missing_ok=True)

    java_home = find_existing_jdk()
    if java_home is None:
        # Fall back to top-level extracted directory name.
        top = {m.name.split("/")[0] for m in members if "/" in m.name}
        raise SystemExit(
            "Extraction finished but no bin/java found. "
            f"Top-level entries: {sorted(top)}"
        )
    print(f"JDK ready at {java_home}")
    return java_home


if __name__ == "__main__":
    home = bootstrap()
    # Print JAVA_HOME so callers can `export JAVA_HOME=$(... )` if desired.
    print(home)
    sys.exit(0)
