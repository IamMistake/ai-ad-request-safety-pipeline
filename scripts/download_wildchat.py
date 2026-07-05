from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

from huggingface_hub import hf_hub_download, list_repo_files


REPO_ID = "allenai/WildChat-4.8M"
OUTPUT_DIR = Path("datasets/WildChat/raw")
SUPPORTED_FORMATS = {"parquet", "jsonl"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download WildChat dataset files into the local raw dataset folder."
    )
    parser.add_argument("--repo-id", default=REPO_ID)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument("--format", choices=sorted(SUPPORTED_FORMATS), default="parquet")
    parser.add_argument(
        "--limit-files",
        type=int,
        default=0,
        help="Download only the first N matching files. Use 0 for all files.",
    )
    return parser.parse_args()


def matching_files(repo_id: str, file_format: str) -> list[str]:
    suffix = f".{file_format}"
    files = [
        path
        for path in list_repo_files(repo_id=repo_id, repo_type="dataset")
        if path.endswith(suffix)
    ]
    if not files:
        raise RuntimeError(f"No {suffix} files found in Hugging Face dataset {repo_id}.")
    return sorted(files)


def copy_dataset_file(repo_id: str, repo_path: str, output_dir: Path) -> Path:
    cached_path = Path(
        hf_hub_download(repo_id=repo_id, repo_type="dataset", filename=repo_path)
    )
    destination = output_dir / repo_path
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(cached_path, destination)
    return destination


def main() -> None:
    args = parse_args()
    files = matching_files(args.repo_id, args.format)
    if args.limit_files > 0:
        files = files[: args.limit_files]

    args.output_dir.mkdir(parents=True, exist_ok=True)
    downloaded = [
        str(copy_dataset_file(args.repo_id, repo_path, args.output_dir))
        for repo_path in files
    ]

    manifest = {
        "repo_id": args.repo_id,
        "format": args.format,
        "output_dir": str(args.output_dir),
        "file_count": len(downloaded),
        "files": downloaded,
    }
    (args.output_dir / "download_manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
