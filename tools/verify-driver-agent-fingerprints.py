#!/usr/bin/env python3
"""校验 driver-agent 指纹与 canonical revision map 一致（发布闸门）。

背景：driver-agent 是独立进程，客户端按 internal/db/driver_agent_revisions_gen.go
里记录的指纹拒绝不匹配的 agent。曾经发生过发布资产里的 agent 指纹落后于
revision map（客户端全部预编译源被拒、被迫回退 8 分钟本地构建）的事故。
本工具在发布流水线里把这类脱节挡在发布之前。

两种模式：

  binaries   静态扫描待发布的 agent 可执行文件，提取内嵌的
             src-<16hex> 指纹，要求与 canonical revision map 一致。
  published  对已发布的 GoNavi-DriverAgents-Manifest.json 做同样比对，
             用于"本次构建不重发驱动资产（has_changes=false）"的场景：
             已发布资产的 revision 必须仍然等于最新 revision map。

用法：
  python3 tools/verify-driver-agent-fingerprints.py binaries \
      --drivers-dir release-assets/drivers \
      --revision-map windows/amd64=map/windows-amd64/driver_agent_revisions_gen.go \
      --revision-map linux/amd64=map/linux-amd64/driver_agent_revisions_gen.go
  python3 tools/verify-driver-agent-fingerprints.py published \
      --manifest GoNavi-DriverAgents-Manifest.json \
      --revision-map windows/amd64=map/windows-amd64/driver_agent_revisions_gen.go

mongodb v1 变体（mongodb-driver-agent-v1-*）不在 revision map 覆盖范围内
（客户端对 v1 跳过校验），只要求其内嵌了指纹；其余 agent 必须逐个等于 map 值。
"""

import argparse
import json
import re
import sys
import zipfile
from pathlib import Path

REVISION_RE = re.compile(rb"src-[0-9a-f]{16}")
AGENT_FILENAME_RE = re.compile(
    r"^(?P<driver>[a-z0-9_]+?)-driver-agent(?:-(?P<variant>v[0-9]+))?"
    r"-(?P<goos>darwin|linux|windows)-(?P<goarch>amd64|arm64)(?:\.exe)?$"
)
GEN_MAP_ENTRY_RE = re.compile(r'"([A-Za-z0-9_]+)":\s*"(src-[0-9a-f]{16})"')


def parse_revision_maps(pairs):
    maps = {}
    for item in pairs:
        if "=" not in item:
            raise SystemExit(f"invalid --revision-map (expected <goos/goarch>=<path>): {item}")
        platform, _, path = item.partition("=")
        platform = platform.strip()
        if platform not in maps and "/" not in platform:
            raise SystemExit(f"invalid platform in --revision-map: {item}")
        text = Path(path).read_text(encoding="utf-8")
        entries = {driver: revision for driver, revision in GEN_MAP_ENTRY_RE.findall(text)}
        if not entries:
            raise SystemExit(f"revision map has no entries: {path}")
        maps[platform] = entries
    if not maps:
        raise SystemExit("at least one --revision-map is required")
    return maps


def collect_release_agent_files(assets_dir):
    """遍历发布产物目录：展开 *.zip 里的 agent 成员，并收录散装 agent 文件。"""
    files = []
    for path in sorted(Path(assets_dir).rglob("*")):
        if not path.is_file():
            continue
        if path.suffix.lower() == ".zip":
            with zipfile.ZipFile(path) as archive:
                for member in archive.infolist():
                    match = AGENT_FILENAME_RE.match(Path(member.filename).name)
                    if match:
                        files.append((f"{path.name}!{member.filename}", match.groupdict(), archive.read(member)))
            continue
        match = AGENT_FILENAME_RE.match(path.name)
        if match:
            files.append((str(path), match.groupdict(), path.read_bytes()))
    return files


def verify_binaries(assets_dir, revision_maps):
    failures = []
    checked = 0
    platform_files = {}
    for label, groups, payload in collect_release_agent_files(assets_dir):
        platform = f"{groups['goos']}/{groups['goarch']}"
        platform_files.setdefault(platform, []).append((label, groups, payload))

    for platform, revision_map in sorted(revision_maps.items()):
        entries = platform_files.get(platform, [])
        if not entries:
            failures.append(f"{platform}: revision map 存在但发布产物里没有该平台的 agent 文件")
            continue
        for label, groups, payload in entries:
            driver = groups["driver"]
            variant = groups["variant"]
            expected = revision_map.get(driver)
            found = sorted({match.decode("ascii") for match in REVISION_RE.findall(payload)})
            label = f"{platform} {label}"
            if not found:
                failures.append(f"{label}: 未内嵌任何 revision 指纹")
                continue
            if len(found) > 1:
                failures.append(f"{label}: 内嵌了多个不同指纹 {found}，无法判定")
                continue
            actual = found[0]
            if driver not in revision_map:
                failures.append(f"{label}: revision map 中没有驱动 {driver} 的条目")
                continue
            if variant == "v1":
                # mongodb v1 变体不在 map 覆盖范围（客户端跳过 v1 校验），只要求已内嵌指纹
                checked += 1
                continue
            if not expected:
                checked += 1
                continue
            checked += 1
            if actual != expected:
                failures.append(
                    f"{label}: 指纹不一致（发布资产={actual}，canonical map 要求={expected}）"
                )

    for platform in sorted(set(platform_files) - set(revision_maps)):
        failures.append(f"{platform}: 发布产物里有该平台的 agent，但未提供对应的 --revision-map")

    return checked, failures


def verify_published(manifest_path, revision_maps):
    failures = []
    checked = 0
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    assets = manifest.get("assets") or {}
    grouped = {}
    for name, asset in assets.items():
        if not isinstance(asset, dict):
            continue
        platform = str(asset.get("platform") or "")
        driver = str(asset.get("driverType") or asset.get("driver") or "")
        revision = str(asset.get("revision") or "")
        if platform and driver:
            grouped.setdefault((platform, driver), {"revisions": set(), "names": []})
            grouped[(platform, driver)]["revisions"].add(revision)
            grouped[(platform, driver)]["names"].append(name)

    for platform, revision_map in sorted(revision_maps.items()):
        for driver, expected in sorted(revision_map.items()):
            if not expected:
                continue
            group = grouped.get((platform, driver))
            if group is None:
                failures.append(
                    f"{platform} {driver}: 已发布清单缺少该驱动资产（期望指纹 {expected}）"
                )
                continue
            checked += 1
            if expected not in group["revisions"]:
                actual = ",".join(sorted(rev or "<empty>" for rev in group["revisions"]))
                failures.append(
                    f"{platform} {driver}: 已发布资产指纹不一致"
                    f"（发布={actual}，canonical map 要求={expected}）"
                )

    for (platform, driver), _group in sorted(grouped.items()):
        if platform in revision_maps and driver not in revision_maps[platform]:
            print(
                f"warning: 已发布清单包含 revision map 未覆盖的驱动（跳过比对）: "
                f"{platform} {driver}",
                file=sys.stderr,
            )

    return checked, failures


def report(mode, checked, failures):
    summary = f"{mode}: 比对 {checked} 个 agent 指纹"
    if failures:
        summary += f"，{len(failures)} 个不一致"
    print(summary)
    for failure in failures:
        print(f"  MISMATCH {failure}")
    if failures:
        print(
            "driver-agent 指纹与 canonical revision map 不一致：请重新发布驱动资产，"
            "否则客户端会拒绝预编译包并回退本地构建。",
            file=sys.stderr,
        )
        return 1
    print("driver-agent 指纹与 canonical revision map 一致")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    subparsers = parser.add_subparsers(dest="mode", required=True)

    binaries = subparsers.add_parser("binaries", help="校验待发布的 agent 可执行文件/zip")
    binaries.add_argument("--release-assets-dir", required=True)
    binaries.add_argument("--revision-map", action="append", default=[],
                          metavar="GOOS/GOARCH=PATH")

    published = subparsers.add_parser("published", help="校验已发布的 driver manifest")
    published.add_argument("--manifest", required=True)
    published.add_argument("--revision-map", action="append", default=[],
                           metavar="GOOS/GOARCH=PATH")

    args = parser.parse_args()
    revision_maps = parse_revision_maps(args.revision_map)

    if args.mode == "binaries":
        if not Path(args.release_assets_dir).is_dir():
            raise SystemExit(f"发布产物目录不存在: {args.release_assets_dir}")
        checked, failures = verify_binaries(args.release_assets_dir, revision_maps)
    else:
        checked, failures = verify_published(args.manifest, revision_maps)

    raise SystemExit(report(args.mode, checked, failures))


if __name__ == "__main__":
    main()
