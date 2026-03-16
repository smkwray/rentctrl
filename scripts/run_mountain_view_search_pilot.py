from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from rent_control_public.mountain_view import (
    authenticate_public_session,
    parse_search_results,
    search_content,
    search_content_count,
    summarize_results,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--keyword', default='301')
    parser.add_argument('--page-size', type=int, default=20)
    args = parser.parse_args()

    processed_dir = ROOT / 'data' / 'processed'
    results_dir = ROOT / 'results' / 'tables'
    processed_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    auth = authenticate_public_session()
    access_token = auth['access_token']
    count_json = search_content_count(args.keyword, access_token=access_token)
    result_json = search_content(args.keyword, access_token=access_token, page_size=args.page_size)

    results = parse_search_results(result_json)
    results['search_keyword'] = args.keyword
    summary = summarize_results(results, keyword=args.keyword, count_json=count_json)

    stem = args.keyword.lower().replace(' ', '_').replace('/', '_')
    results_path = processed_dir / f'mountain_view_search_{stem}_results.csv'
    summary_path = results_dir / f'mountain_view_search_{stem}_summary.csv'
    counts_path = results_dir / f'mountain_view_search_{stem}_count_response.json'
    raw_path = results_dir / f'mountain_view_search_{stem}_raw_response.json'

    results.to_csv(results_path, index=False)
    summary.to_csv(summary_path, index=False)
    counts_path.write_text(__import__('json').dumps(count_json, indent=2))
    raw_path.write_text(__import__('json').dumps(result_json, indent=2))

    print(f'wrote {results_path}')
    print(f'wrote {summary_path}')
    print(summary.to_string(index=False))


if __name__ == '__main__':
    main()
