"""Build a deterministic, content-addressed Ray working-directory archive."""

import hashlib
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

ROOT = Path(__file__).parent
DIST = ROOT / "dist"
ARCHIVE_SOURCES = [ROOT / "setup.py", *sorted((ROOT / "trainer").glob("*.py"))]


def _archive_bytes(path: Path) -> None:
    with ZipFile(path, "w", compression=ZIP_DEFLATED, compresslevel=9) as archive:
        for source in ARCHIVE_SOURCES:
            relative = source.relative_to(ROOT).as_posix()
            info = ZipInfo(relative, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, source.read_bytes(), compress_type=ZIP_DEFLATED, compresslevel=9)


def main() -> None:
    DIST.mkdir(exist_ok=True)
    temporary = DIST / "test_ctr_model_ray.building.zip"
    _archive_bytes(temporary)
    digest = hashlib.sha256(temporary.read_bytes()).hexdigest()
    target = DIST / f"test_ctr_model_ray-{digest[:12]}.zip"
    temporary.replace(target)
    (DIST / f"{target.name}.sha256").write_text(f"{digest}  {target.name}\n")
    print(target)
    print(digest)


if __name__ == "__main__":
    main()
