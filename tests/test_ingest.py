from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pytest

from deduplicate.ingest_file import ingest_file
from deduplicate.snippet_handling import (
    concatenate_snippets,
    dedupe_snippets,
    split_text_to_snippets,
)



@pytest.fixture(scope='module')
def test_data() -> list[str]:
    path = Path(__file__).parent / "test_data.txt"
    return path.open('r').readlines()


def test_ingest(test_data: list[str]):
    ingested =  ingest_file(test_data)
    example = ingested[0]
    for example in ingested:
        assert "url" in example
        assert "text" in example
        assert "*" not in example["url"]
        assert " " not in example["url"]

def test_dedup(test_data):
    ingested = ingest_file(test_data)
    snippets = split_text_to_snippets(ingested)
    
    deduped = dedupe_snippets(snippets)
    pass

def test_combine_snippets(test_data: list[str]):
    ingested = ingest_file(test_data)
    ingested_dicts = [webpage.__dict__ for webpage in ingested]
    ingested_df = pl.DataFrame(ingested_dicts).sort("url")
    
    snippets = split_text_to_snippets(ingested)
    snippets_dicts = [snippet.__dict__ for snippet in snippets]
    
    concatenated = concatenate_snippets(snippets_dicts)
    
    assert concatenated["text"][0] == ingested_df["text"][0]

