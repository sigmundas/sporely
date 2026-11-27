"""Static checks on the release pipeline (audit finding F26).

The workflows cannot run locally, so these tests parse them and the build
scripts and assert the supply-chain properties: least-privilege tokens,
actions pinned by commit SHA, hash-locked installs and a checksum asset.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
RELEASE = WORKFLOWS / "release.yml"
CI = WORKFLOWS / "ci.yml"
LOCK = ROOT / "requirements.lock"
TEST_LOCK = ROOT / "requirements-test.lock"
BUILD_SCRIPTS = [ROOT / "build_mac.sh", ROOT / "build_linux.sh", ROOT / "build.ps1"]

SHA_RE = re.compile(r"^[^@\s]+@[0-9a-f]{40}$")
# validate-version is the only read-only job; verify-draft needs write because
# GitHub shows draft releases to write access only.
WRITE_JOBS = {
    "create-draft", "build-windows", "build-linux", "build-macos",
    "checksums", "verify-draft",
}


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _steps(workflow: dict):
    for job_name, job in workflow["jobs"].items():
        for step in job.get("steps", []):
            yield job_name, step


def _pip_install_lines(text: str):
    for raw in text.splitlines():
        line = raw.strip()
        if re.search(r"\bpip\s+install\b", line):
            yield line


def test_release_workflow_default_permissions_are_read_only():
    perms = _load(RELEASE).get("permissions")
    assert perms == {"contents": "read"}


def test_only_upload_jobs_have_contents_write():
    jobs = _load(RELEASE)["jobs"]
    writers = {
        name for name, job in jobs.items()
        if (job.get("permissions") or {}).get("contents") == "write"
    }
    assert writers == WRITE_JOBS
    for name, job in jobs.items():
        perms = job.get("permissions") or {}
        assert set(perms) <= {"contents"}, f"{name} grants extra scopes"


def test_every_action_is_pinned_to_a_full_commit_sha():
    for path in (RELEASE, CI):
        found = 0
        for job_name, step in _steps(_load(path)):
            uses = step.get("uses")
            if uses:
                found += 1
                assert SHA_RE.match(uses), f"{path.name}:{job_name}: {uses}"
        assert found
        # The tag stays readable next to each pin.
        for line in path.read_text(encoding="utf-8").splitlines():
            if "uses:" in line and not line.strip().startswith("#"):
                assert re.search(r"#\s*v\d", line), f"{path.name}: no tag comment: {line}"


def test_release_installs_are_hash_locked():
    sources = [RELEASE.read_text(encoding="utf-8")] + [
        p.read_text(encoding="utf-8") for p in BUILD_SCRIPTS
    ]
    count = 0
    for text in sources:
        for line in _pip_install_lines(text):
            count += 1
            assert "--require-hashes" in line, line
            assert "requirements.lock" in line, line
    assert count >= 4


def test_ci_installs_are_hash_locked():
    lines = list(_pip_install_lines(CI.read_text(encoding="utf-8")))
    assert lines
    for line in lines:
        assert "--require-hashes" in line, line


def _lock_requirements(path: Path):
    """Return one text block (requirement line plus hash lines) per package."""
    blocks, current = [], None
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        if not raw[0].isspace():
            current = [raw]
            blocks.append(current)
        elif current is not None:
            current.append(raw)
    return ["\n".join(b) for b in blocks]


def test_lock_files_exist_and_every_requirement_has_hashes():
    for path in (LOCK, TEST_LOCK):
        assert path.is_file(), path
        blocks = _lock_requirements(path)
        assert blocks
        for block in blocks:
            assert "--hash=sha256:" in block, block
            assert "==" in block.splitlines()[0], block


def test_lock_keeps_existing_exact_pins_and_covers_build_tools():
    text = LOCK.read_text(encoding="utf-8").lower()
    assert re.search(r"^pyside6==6\.10\.3", text, re.M)
    assert re.search(r"^pyyaml==6\.0\.3", text, re.M)
    assert re.search(r"^pillow==", text, re.M)
    assert re.search(r"^pyinstaller==", text, re.M)
    # Platform-specific packages must be covered (universal lock).
    assert "sys_platform == 'win32'" in text


def test_release_publishes_and_checks_sha256sums():
    text = RELEASE.read_text(encoding="utf-8")
    assert "SHA256SUMS" in text
    jobs = _load(RELEASE)["jobs"]
    verify = "\n".join(str(s.get("run", "")) for s in jobs["verify-draft"]["steps"])
    assert "SHA256SUMS" in verify


def test_release_checkouts_do_not_persist_the_token():
    count = 0
    for job_name, step in _steps(_load(RELEASE)):
        if str(step.get("uses", "")).startswith("actions/checkout@"):
            count += 1
            assert step.get("with", {}).get("persist-credentials") is False, job_name
    assert count == 4
