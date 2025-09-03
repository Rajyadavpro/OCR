import os
import argparse
from datetime import datetime


def consolidate_folder(folder: str, output_filename: str = None) -> int:
    """Concatenate all .txt files directly inside `folder` into a consolidated file in the same folder.

    Returns number of files concatenated (0 if none).
    """
    folder = os.path.abspath(folder)
    if not os.path.isdir(folder):
        return 0

    # Find txt files only in this folder (not recursive)
    txt_files = [f for f in os.listdir(folder)
                 if os.path.isfile(os.path.join(folder, f)) and f.lower().endswith('.txt') and not f.startswith('consolidated_')]

    if not txt_files:
        return 0

    ts = datetime.now().strftime('%Y%m%dT%H%M%S')
    out_name = output_filename or f'consolidated_{ts}.txt'
    output_path = os.path.join(folder, out_name)

    with open(output_path, 'w', encoding='utf-8', errors='replace') as out:
        out.write(f"Consolidated file created: {datetime.now().isoformat()}\n")
        out.write(f"Source folder: {folder}\n")
        out.write(f"Total txt files found: {len(txt_files)}\n")
        out.write("\n---- START OF FILES ----\n\n")

        for idx, fname in enumerate(sorted(txt_files)):
            path = os.path.join(folder, fname)
            out.write(f"\n===== File {idx+1}/{len(txt_files)}: {fname} =====\n")
            try:
                with open(path, 'r', encoding='utf-8', errors='replace') as inp:
                    out.write(inp.read())
            except Exception as e:
                out.write(f"\n[ERROR reading file: {e}]\n")

    return len(txt_files)


def consolidate_artifacts_subfolders(artifacts_dir: str) -> int:
    """Walk artifacts_dir and consolidate .txt files inside each subfolder (including root if it has txt files).

    Returns number of consolidated files created (one per folder that had txt files).
    """
    artifacts_dir = os.path.abspath(artifacts_dir)
    if not os.path.isdir(artifacts_dir):
        raise FileNotFoundError(f"Artifacts directory not found: {artifacts_dir}")

    created = 0

    # Walk through directories; os.walk yields root first. Only process subfolders (skip artifacts_dir itself).
    for root, dirs, files in os.walk(artifacts_dir):
        if os.path.abspath(root) == os.path.abspath(artifacts_dir):
            # skip the root artifacts folder; only consolidate inside subfolders
            continue
        # Consolidate only files within this sub-directory (not merging files from child dirs)
        num = consolidate_folder(root)
        if num > 0:
            created += 1

    return created


def main():
    parser = argparse.ArgumentParser(description='Create consolidated .txt inside each artifacts subfolder that contains .txt files')
    parser.add_argument('--artifacts', '-a', help='Artifacts directory (default: ./artifacts)', default='artifacts')
    args = parser.parse_args()

    artifacts_dir = os.path.abspath(args.artifacts)
    print(f"Scanning artifacts directory: {artifacts_dir}")

    created = consolidate_artifacts_subfolders(artifacts_dir)
    print(f"Completed. Created consolidated files in {created} folder(s).")


if __name__ == '__main__':
    main()
