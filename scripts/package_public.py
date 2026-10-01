"""Export only reviewed publication candidates; never include Git history."""
import argparse
import zipfile
from pathlib import Path
from check_public_files import ROOT, public_files, validate_files

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--output", type=Path, default=ROOT.parent / "media-data-pipeline-public.zip")
args = parser.parse_args()
if ROOT == args.output.resolve() or ROOT in args.output.resolve().parents:
    parser.error("Write the archive outside the repository")
files = public_files()
findings = validate_files(files)
if findings:
    raise SystemExit("\n".join(findings))
args.output.parent.mkdir(parents=True, exist_ok=True)
with zipfile.ZipFile(args.output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
    for path in files:
        archive.write(path, path.relative_to(ROOT).as_posix())
print(f"Public archive written with {len(files)} files: {args.output}")
