
import polars as pl

from deduplicate.ingest_file import ingest_file
from deduplicate.snippet_handling import (
    concatenate_snippets,
    dedupe_snippets,
    split_text_to_snippets,
)


def test_ingest(test_data_path):
    ingested =  ingest_file(test_data_path)
    example = ingested[0]
    
    for example in ingested:
        assert example.url != "" 
        assert "*" not in example.url
        assert example.url is not None
        assert " "  not in example.url
        
        assert example.text != "" and example.text is not None

def test_dedup(test_data_path):
    ingested = ingest_file(test_data_path)
    snippets = split_text_to_snippets(ingested)
    
    deduped = dedupe_snippets(snippets)
    pass

def test_combine_snippets(test_data_path):
    ingested = ingest_file(test_data_path)
    ingested_dicts = [webpage.__dict__ for webpage in ingested]
    ingested_df = pl.DataFrame(ingested_dicts).sort("url")
    
    snippets = split_text_to_snippets(ingested)
    snippets_dicts = [snippet.__dict__ for snippet in snippets]
    
    concatenated = concatenate_snippets(snippets_dicts).sort("url")
    
    assert concatenated["text"][0] == ingested_df["text"][0]


