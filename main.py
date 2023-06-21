from pathlib import Path
from typing import Sequence

import polars as pl

from deduplicate.ingest_file import ingest_file
from deduplicate.snippet_handling import (
    concatenate_snippets,
    dedupe_snippets,
    split_text_to_snippets,
)


def main(file_paths: Sequence[Path]):
    raw_dir = Path(__file__).parent / "raw"
    deduped_dir = Path(__file__).parent / "deduped"

    for dir in (raw_dir, deduped_dir):
        dir.mkdir(exist_ok=True, parents=True)
    
    for path in file_paths:
        deduped_df, webpages_df = dedup_file(path)
        deduped_df.write_ndjson(deduped_dir / f"{path.stem}_deduped.ndjson")
        webpages_df.write_ndjson(raw_dir / f"{path.stem}_raw.ndjson")
        

        

def dedup_file(path):
    file = ingest_file(path)
    snippets = split_text_to_snippets(file)
    deduped_snippets = dedupe_snippets(snippets=snippets)
    deduped_df = concatenate_snippets(snippets_dicts=deduped_snippets)
    
    webpages_df = pl.DataFrame([webpage.__dict__ for webpage in file])
    return deduped_df, webpages_df

if __name__ == "__main__":
    data_path = Path(__file__).parent.parent / "795173"

    main(file_paths=list(data_path.glob("*.txt")))