"""Port allocation contract (install.sh alloc_port/release_port helpers):
smallest unregistered port from the 8644 base, same-service idempotent
reuse, atomic registry writes, and service-matched release.

The helpers live inside install.sh as bash functions wrapping python
heredocs; like the detect_lan_ip probe, they are extracted with sed and
sourced so the SAME code the installer runs is what gets tested.
"""
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

PKG_DIR = Path(__file__).resolve().parent.parent
INSTALL = PKG_DIR / "install.sh"


def _source_functions(tmp_path):
    """Extract the alloc/release helpers + die/note into a sourceable file."""
    func = tmp_path / "funcs.sh"
    text = INSTALL.read_text(encoding="utf-8")
    out = []
    for name in ("alloc_port", "release_port"):
        start = text.index(f"{name}() {{")
        end = text.index("\n}", start) + 2
        out.append(text[start:end])
    func.write_text("#!/usr/bin/env bash\n" + "\n\n".join(out) + "\n", encoding="utf-8")
    return func


def _alloc(func, registry, service, base=8644):
    out = subprocess.run(
        ["bash", "-c", f'source "{func}"; alloc_port "{registry}" "{service}" "{base}"'],
        capture_output=True, text=True, check=True)
    return int(out.stdout.strip())


def _release(func, registry, port, service=""):
    subprocess.run(
        ["bash", "-c", f'source "{func}"; release_port "{registry}" "{port}" "{service}"'],
        capture_output=True, text=True, check=True)


def _registry(registry):
    return json.loads(Path(registry).read_text(encoding="utf-8"))


def test_first_allocation_takes_the_base_port(tmp_path):
    func = _source_functions(tmp_path)
    registry = tmp_path / "ports.json"
    assert _alloc(func, registry, "svc-a") == 8644
    assert _registry(registry) == {"8644": "svc-a"}


def test_second_service_skips_to_next_port(tmp_path):
    func = _source_functions(tmp_path)
    registry = tmp_path / "ports.json"
    assert _alloc(func, registry, "svc-a") == 8644
    assert _alloc(func, registry, "svc-b") == 8645
    assert _registry(registry) == {"8644": "svc-a", "8645": "svc-b"}


def test_same_service_reuses_its_registered_port(tmp_path):
    func = _source_functions(tmp_path)
    registry = tmp_path / "ports.json"
    assert _alloc(func, registry, "svc-a") == 8644
    assert _alloc(func, registry, "svc-b") == 8645
    assert _alloc(func, registry, "svc-a") == 8644, "idempotent re-install"
    assert _registry(registry) == {"8644": "svc-a", "8645": "svc-b"}


def test_allocation_fills_holes_left_by_releases(tmp_path):
    func = _source_functions(tmp_path)
    registry = tmp_path / "ports.json"
    _alloc(func, registry, "svc-a")            # 8644
    _alloc(func, registry, "svc-b")            # 8645
    _release(func, registry, "8644", "svc-a")  # hole at the base
    assert _alloc(func, registry, "svc-c") == 8644, "smallest free wins"


def test_release_is_service_matched_and_missing_files_are_tolerated(tmp_path):
    func = _source_functions(tmp_path)
    registry = tmp_path / "ports.json"
    _alloc(func, registry, "svc-a")
    _release(func, registry, "8644", "someone-else")  # wrong owner: kept
    assert _registry(registry) == {"8644": "svc-a"}
    _release(func, registry, "8644", "svc-a")         # right owner: gone
    assert _registry(registry) == {}
    _release(func, registry, "8644", "svc-a")         # idempotent no-op
    missing = tmp_path / "nested" / "ports.json"
    _release(func, missing, "8644")                   # absent registry: fine
    assert not missing.exists()


def test_registry_write_is_atomic_no_tmp_left_behind(tmp_path):
    func = _source_functions(tmp_path)
    registry = tmp_path / "ports.json"
    _alloc(func, registry, "svc-a")
    leftovers = [p.name for p in tmp_path.iterdir() if p.name.endswith(".tmp")]
    assert leftovers == [], f"stray tmp files: {leftovers}"
    assert json.loads(registry.read_text()) is not None
