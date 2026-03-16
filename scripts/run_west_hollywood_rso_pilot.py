"""West Hollywood rent-stabilized address pilot.

Downloads the public RSO address list PDF, parses it, and writes a summary
table.  Accepts ``--pdf-path`` to use a manually-downloaded PDF when the
city's CDN blocks automated retrieval.

Exits with a clear blocker note if pdftotext is unavailable or the PDF
cannot be fetched.
"""
from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from rent_control_public.west_hollywood import (
    RSO_PAGE_URL,
    WeHoDownloadError,
    download_rso_pdf,
    extract_pdf_text,
    parse_rso_text,
    summarize_rso_stock,
)

RESULTS_DIR = ROOT / "results" / "tables"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--pdf-path",
        type=Path,
        default=None,
        help=(
            "Path to a locally-downloaded RSO PDF.  Use this when the "
            "city's CDN blocks automated downloads."
        ),
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)

    # Check pdftotext availability
    if shutil.which("pdftotext") is None:
        print("BLOCKER: pdftotext is not installed. Install poppler-utils to proceed.")
        sys.exit(1)

    if args.pdf_path is not None:
        pdf_path = args.pdf_path.expanduser().resolve()
        if not pdf_path.is_file():
            print(f"BLOCKER: --pdf-path {pdf_path} does not exist.")
            sys.exit(1)
        print(f"Using local PDF: {pdf_path}")
        _run_pipeline(pdf_path)
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            pdf_path = Path(tmpdir) / "weho_rso_addresses.pdf"
            print("Downloading West Hollywood RSO address list PDF...")
            try:
                download_rso_pdf(pdf_path)
            except WeHoDownloadError as exc:
                print(f"BLOCKER: {exc}")
                sys.exit(1)
            except Exception as exc:
                print(f"BLOCKER: Unexpected download error: {exc}")
                sys.exit(1)
            _run_pipeline(pdf_path)


def _run_pipeline(pdf_path: Path) -> None:
    print("Extracting text from PDF...")
    text = extract_pdf_text(pdf_path)

    print("Parsing rent-stabilized addresses...")
    df = parse_rso_text(text)

    if df.empty:
        print("WARNING: No address rows parsed from PDF. The format may have changed.")
        print("Writing empty summary.")

    summary = summarize_rso_stock(df)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    detail_path = RESULTS_DIR / "weho_rso_stock.csv"
    out_path = RESULTS_DIR / "weho_rso_stock_summary.csv"
    df.to_csv(detail_path, index=False)
    summary.to_csv(out_path, index=False)
    print(f"Detail written to {detail_path}")
    print(f"Summary written to {out_path}")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
