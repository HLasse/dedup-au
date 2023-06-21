from typing import Sequence

import spacy

from tests.test_ingest import Snippet


def split_text_to_snippets(ingested_data):
    nlp = spacy.blank("da")
    nlp.add_pipe("sentencizer")

    snippets = []
    for webpage in ingested_data:
        doc = nlp(webpage.text)
        snippets.extend([Snippet(url=webpage.url, text=sent.text, index=i) for i, sent in enumerate(doc.sents)])
    return snippets

def dedupe_snippets(snippets: Sequence[Snippet]):
    from nlp_dedup import Deduper
    deduper = Deduper(store_corpus_to_disk=False, store_config_to_disk=False, store_lsh_cache_to_disk=False, store_mask_to_disk=False, return_generator=True)

    snippets_dicts: list[dict] = [snippet.__dict__ for snippet in snippets]

    dedupe_mask = deduper.deduplicate(corpus=snippets_dicts, overwrite=True)

    deduped = []
    duplicates = []

    for snippet, mask_dict in zip(snippets, dedupe_mask):
        duplicate = mask_dict["duplicate"]
        if not duplicate:
            deduped.append(snippet)
        else:
            duplicates.append(snippet)

    return deduped



def concatenate_snippets(snippets_dicts: list[dict]):
    import polars as pl
    snippets_df = pl.DataFrame(snippets_dicts)

    concatenated = snippets_df.groupby("url").agg(pl.col("text").str.concat(" "))
    return concatenated



