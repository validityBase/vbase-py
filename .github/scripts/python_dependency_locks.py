"""Manage source-first Python dependency lock updates for GitHub Actions."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path


LOCK_SPECS = (
    (
        ("--allow-unsafe",),
        "requirements/lock/dev.txt",
        "requirements/src/dev.in",
    ),
    (
        (),
        "requirements/lock/test.txt",
        "requirements/src/test.in",
    ),
    (
        (),
        "requirements/lock/docs.txt",
        "requirements/src/docs.in",
    ),
    (
        ("--allow-unsafe",),
        "requirements/lock/tools.txt",
        "requirements/src/tools.in",
    ),
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Update source requirements and generated lock files."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    update_parser = subparsers.add_parser("update-sources")
    update_parser.add_argument("--dependency", default="")
    update_parser.add_argument("--constraint", default="")
    update_parser.add_argument("--source-files", required=True)
    update_parser.set_defaults(func=update_sources)

    compile_parser = subparsers.add_parser("compile-locks")
    compile_parser.add_argument("--upgrade", default="false")
    compile_parser.set_defaults(func=compile_locks)

    pr_parser = subparsers.add_parser("open-pr")
    pr_parser.add_argument("--dependency", default="")
    pr_parser.add_argument("--base-branch", required=True)
    pr_parser.add_argument("--run-id", required=True)
    pr_parser.set_defaults(func=open_pr)

    args = parser.parse_args()
    args.func(args)
    return 0


def update_sources(args: argparse.Namespace) -> None:
    dependency = args.dependency.strip()
    constraint = args.constraint.strip()
    source_files = args.source_files.split()

    if bool(dependency) != bool(constraint):
        sys.exit("dependency and constraint must be provided together")

    if not dependency:
        print("No dependency constraint requested; regenerating locks only.")
        return

    requirement = build_requirement(dependency, constraint)

    for source_file in source_files:
        path = validate_source_file(source_file)
        lines = path.read_text(encoding="utf-8").splitlines()
        lines = upsert_requirement(lines, dependency, requirement)
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"Updated {path}: {requirement}")


def build_requirement(dependency: str, constraint: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", dependency):
        sys.exit(f"Invalid dependency name: {dependency}")
    if "\n" in constraint or "\r" in constraint:
        sys.exit("constraint must be a single line")

    if re.match(r"^[<>=!~]", constraint):
        return f"{dependency}{constraint}"

    candidate = split_requirement_name(constraint)
    if normalize_name(candidate) != normalize_name(dependency):
        sys.exit("constraint must start with a version operator or dependency name")

    return constraint


def validate_source_file(source_file: str) -> Path:
    path = Path(source_file.strip())

    if not source_file.strip():
        sys.exit("Requirement source file path must not be empty")
    if path.is_absolute() or any(part in {".", ".."} for part in path.parts):
        sys.exit(f"Refusing to edit unsafe path: {source_file}")
    if path == Path("requirements.in"):
        if not path.exists():
            sys.exit(f"Requirement source file does not exist: {source_file}")
        return path
    if path.parts[:2] != ("requirements", "src") or path.suffix != ".in":
        sys.exit(f"Refusing to edit non-source requirement file: {source_file}")
    if not path.exists():
        sys.exit(f"Requirement source file does not exist: {source_file}")

    source_root = (Path.cwd() / "requirements" / "src").resolve()
    resolved_path = path.resolve()
    try:
        resolved_path.relative_to(source_root)
    except ValueError:
        sys.exit(f"Refusing to edit source file outside requirements/src: {source_file}")

    return path


def upsert_requirement(
    lines: list[str], dependency: str, requirement: str
) -> list[str]:
    dependency_key = normalize_name(dependency)

    for index, line in enumerate(lines):
        stripped = line.strip()
        if not stripped or stripped.startswith(("#", "-")):
            continue

        if normalize_name(split_requirement_name(stripped)) == dependency_key:
            lines[index] = requirement
            return lines

    if lines and lines[-1]:
        lines.append("")
    lines.append(requirement)
    return lines


def compile_locks(args: argparse.Namespace) -> None:
    upgrade_args = ["--upgrade"] if parse_bool(args.upgrade) else []

    for extra_args, output_file, source_file in LOCK_SPECS:
        run(
            [
                "pip-compile",
                *upgrade_args,
                "--strip-extras",
                "--no-annotate",
                *extra_args,
                "--generate-hashes",
                "-o",
                output_file,
                source_file,
            ]
        )


def open_pr(args: argparse.Namespace) -> None:
    if not has_dependency_diff():
        print("No dependency lock changes to publish.")
        return

    dependency = args.dependency.strip()
    dependency_slug = slugify(dependency or "locks")
    branch = f"automation/python-dependency-locks-{dependency_slug}-{args.run_id}"
    title = "Update Python dependency locks"
    if dependency:
        title = f"Update {dependency} Python dependency locks"

    run(["git", "config", "user.name", "github-actions[bot]"])
    run(
        [
            "git",
            "config",
            "user.email",
            "41898282+github-actions[bot]@users.noreply.github.com",
        ]
    )
    run(["git", "checkout", "-b", branch])
    run(["git", "add", "requirements.in", "requirements/src", "requirements/lock"])
    run(["git", "commit", "-m", title])
    run(["git", "push", "--set-upstream", "origin", branch])

    with tempfile.NamedTemporaryFile("w", encoding="utf-8") as body_file:
        body_file.write(
            "\n".join(
                (
                    "## Summary",
                    "",
                    "- updates Python dependency source constraints when requested",
                    "- regenerates hash-locked requirements with pinned pip-tools",
                    "",
                    "## Validation",
                    "",
                    "- regenerated requirements/lock/*.txt from source requirements",
                    "",
                )
            )
        )
        body_file.flush()
        run(
            [
                "gh",
                "pr",
                "create",
                "--base",
                args.base_branch,
                "--head",
                branch,
                "--title",
                title,
                "--body-file",
                body_file.name,
            ]
        )


def has_dependency_diff() -> bool:
    result = subprocess.run(
        [
            "git",
            "diff",
            "--quiet",
            "--",
            "requirements.in",
            "requirements/src",
            "requirements/lock",
        ],
        check=False,
    )
    if result.returncode == 0:
        return False
    if result.returncode == 1:
        return True
    raise subprocess.CalledProcessError(result.returncode, result.args)


def normalize_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def split_requirement_name(requirement: str) -> str:
    return re.split(r"[<>=!~; \[]", requirement, maxsplit=1)[0]


def parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes"}


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9._-]", "-", value.lower())


def run(command: list[str]) -> None:
    print("+ " + " ".join(command))
    subprocess.run(command, check=True)


if __name__ == "__main__":
    raise SystemExit(main())
