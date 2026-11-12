import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import signal
import subprocess
import tarfile
import time
import urllib.request

root = Path(__file__).resolve().parent.parent
cache = root / ".local/plugin-tools"
pins = json.loads((root / "scripts/plugin-tools.json").read_text())
builder = pins["builder"]
deadline = time.monotonic() + 900
owned = {}
cancelled = False


def digest(data):
    return hashlib.sha256(data).hexdigest()


def verify(path, expected):
    if digest(path.read_bytes()) != expected:
        raise RuntimeError("PLUGIN_TOOL_HASH")


def stop(sig=None, frame=None):
    global cancelled
    cancelled = True
    for pid in list(owned):
        try:
            os.killpg(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def budget():
    if cancelled or time.monotonic() > deadline:
        raise RuntimeError("PLUGIN_TOOL_DEADLINE")
    if shutil.disk_usage(cache).free < 4 * 1024 ** 3:
        raise RuntimeError("PLUGIN_TOOL_DISK_FREE")
    size = sum(p.stat().st_size for p in cache.rglob("*") if p.is_file())
    if size > 8 * 1024 ** 3:
        raise RuntimeError("PLUGIN_TOOL_DISK_BUDGET")


def run(command, cwd, env):
    budget()
    with (cache / "setup.log").open("ab", buffering=0) as log:
        child = subprocess.Popen(command, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                                 stdout=log, stderr=log, start_new_session=True)
        owned[child.pid] = child
        try:
            next_sample = 0
            while child.poll() is None:
                if time.monotonic() > next_sample:
                    budget()
                    next_sample = time.monotonic() + 2
                if (cache / "setup.log").stat().st_size > 8 * 1024 ** 2:
                    raise RuntimeError("PLUGIN_TOOL_OUTPUT_BUDGET")
                time.sleep(0.1)
            if child.returncode:
                raise RuntimeError("PLUGIN_TOOL_COMMAND")
        finally:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGKILL)
            child.wait()
            owned.pop(child.pid, None)


def acquire(name, pin, allowed):
    path = cache / "vendor" / name
    if not path.exists():
        if not allowed:
            raise RuntimeError("PLUGIN_TOOL_MISSING_USE_FETCH_TOOLS")
        temporary = path.with_suffix(".download")
        try:
            with urllib.request.urlopen(pin["url"], timeout=30) as response, temporary.open("wb") as out:
                size = 0
                while True:
                    budget()
                    chunk = response.read(65536)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > 32 * 1024 ** 2:
                        raise RuntimeError("PLUGIN_TOOL_DOWNLOAD_BUDGET")
                    out.write(chunk)
            verify(temporary, pin["sha256"])
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)
    verify(path, pin["sha256"])
    return path


def extract_engine(binary):
    if digest(binary) != builder["officialBinarySha256"]:
        raise RuntimeError("PLUGIN_OFFICIAL_BINARY_HASH")
    spec = builder["engine"]
    engine = binary[spec["offset"]:spec["offset"] + spec["bytes"]]
    if digest(engine) != spec["sha256"] or engine[:8] != b"\0asm\1\0\0\0":
        raise RuntimeError("PLUGIN_ENGINE_HASH")
    cursor = 8
    while cursor < len(engine):
        if engine[cursor] > 12:
            raise RuntimeError("PLUGIN_ENGINE_SECTION")
        cursor += 1
        size = 0
        for shift in range(0, 35, 7):
            byte = engine[cursor]
            cursor += 1
            size |= (byte & 127) << shift
            if byte < 128:
                break
        else:
            raise RuntimeError("PLUGIN_ENGINE_SECTION")
        cursor += size
    if cursor != len(engine):
        raise RuntimeError("PLUGIN_ENGINE_BOUNDARY")
    (cache / "engine.wasm").write_bytes(engine)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fetch-tools", action="store_true")
    parser.add_argument("--fetch-dependencies", action="store_true")
    parser.add_argument("--rust-bin", type=Path)
    parser.add_argument("--import-tool", type=Path)
    args = parser.parse_args()
    cache.mkdir(parents=True, exist_ok=True)
    (cache / "vendor").mkdir(exist_ok=True)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    patch = root / "scripts/plugin-builder/deterministic-preinit.patch"
    verify(patch, builder["patchSha256"])
    verify(root / "scripts/plugin-builder/upstream-Cargo.lock", builder["upstreamLockSha256"])
    verify(root / "scripts/plugin-builder/Cargo.lock", builder["lockSha256"])
    official = gzip.decompress(acquire("extism-js.gz", pins["extism-js"], args.fetch_tools).read_bytes())
    extract_engine(official)
    archive = acquire("js-pdk-source.tar.gz", builder["source"], args.fetch_tools)
    source_parent = cache / "builder-source"
    marker = source_parent / ".gateway-owned"
    if source_parent.exists():
        if not marker.exists() or marker.read_text() != builder["source"]["sha256"]:
            raise RuntimeError("PLUGIN_SOURCE_OWNERSHIP")
        shutil.rmtree(source_parent)
    source_parent.mkdir()
    marker.write_text(builder["source"]["sha256"])
    with tarfile.open(archive, "r:gz") as package:
        members = package.getmembers()
        if len(members) > 4096 or sum(item.size for item in members) > 16 * 1024 ** 2:
            raise RuntimeError("PLUGIN_SOURCE_BUDGET")
        for item in members:
            path = PurePosixPath(item.name)
            if path.is_absolute() or ".." in path.parts or not path.parts or path.parts[0] != "js-pdk-1.7.0" or not (item.isfile() or item.isdir()):
                raise RuntimeError("PLUGIN_SOURCE_PATH")
        package.extractall(source_parent, members=members)
    source = source_parent / "js-pdk-1.7.0"
    verify(source / "Cargo.lock", builder["upstreamLockSha256"])
    clean = {"PATH": "/usr/bin:/bin"}
    run(["/usr/bin/patch", "--batch", "--forward", "-p1", "-i", str(patch)], source, clean)
    verify(source / "Cargo.lock", builder["lockSha256"])
    target = cache / "extism-js-deterministic"
    if args.import_tool:
        if args.import_tool.stat().st_size > 64 * 1024 ** 2:
            raise RuntimeError("PLUGIN_TOOL_BINARY_BUDGET")
        verify(args.import_tool, builder["referenceBinarySha256"])
        shutil.copyfile(args.import_tool, target)
        origin = "verified-reference-binary"
    else:
        if not args.rust_bin or not args.rust_bin.is_absolute():
            raise RuntimeError("PLUGIN_RUST_BIN_REQUIRED")
        rustc, cargo = args.rust_bin / "rustc", args.rust_bin / "cargo"
        version = subprocess.check_output([str(rustc), "--version"], env=clean, timeout=10).decode()
        if not version.startswith("rustc " + builder["rustVersion"] + " "):
            raise RuntimeError("PLUGIN_RUST_VERSION")
        env = {"PATH": str(args.rust_bin) + ":/usr/bin:/bin", "CARGO_HOME": str(cache / "cargo-home"),
               "RUSTUP_HOME": str(cache / "rustup-home"), "CARGO_TARGET_DIR": str(cache / "target"),
               "RUSTC": str(rustc), "EXTISM_ENGINE_PATH": str(cache / "engine.wasm"),
               "CARGO_PROFILE_RELEASE_OPT_LEVEL": "0", "CARGO_PROFILE_RELEASE_DEBUG": "0",
               "CARGO_HTTP_TIMEOUT": "30", "CARGO_NET_RETRY": "2"}
        if args.fetch_dependencies:
            run([str(cargo), "fetch", "--locked", "--target", "aarch64-apple-darwin"], source, env)
        run([str(cargo), "build", "--frozen", "--release", "-p", "js-pdk-cli", "-j", "2"], source, env)
        shutil.copyfile(cache / "target/release/extism-js", target)
        origin = "verified-source-build"
    target.chmod(0o700)
    receipt = {"format": 1, "origin": origin, "binarySha256": digest(target.read_bytes()),
               "builder": builder}
    (cache / "builder-receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({"builder": builder["version"], "origin": origin, "sha256": receipt["binarySha256"]}))


try:
    main()
finally:
    stop()
    for child in list(owned.values()):
        child.wait()
