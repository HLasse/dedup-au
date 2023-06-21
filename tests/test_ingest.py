from dataclasses import dataclass
from pathlib import Path

import pytest
import spacy


@dataclass
class Webpage:
    url: str
    text: str

@dataclass
class Snippet:
    url: str
    text: str
    index: int


@pytest.fixture(scope='module')
def test_data():
    path = Path(__file__).parent / 'test_data.txt'
    return path.open('r').readlines()


def test_ingest(test_data):
    ingested =  ingest_file(test_data)
    example = ingested[0]
    for example in ingested:
        assert "url" in example
        assert "text" in example
        assert "*" not in example["url"]
        assert " " not in example["url"]

def ingest_file(test_data: list[str]):
    output_webpages  = []
    url = None
    for line in test_data:
        if line.startswith("***"):
            chunks = line.split(" ")
            url = [chunk for chunk in chunks if chunk.startswith("http")][0]
        elif len(line) > 2:
            if url is None:
                raise ValueError("URL not set")
            output_webpages.append(Webpage(url=url, text=line))
            url = None
    return output_webpages


def test_snippets(test_data):
    ingested = ingest_file(test_data)
    snippets = split_text_to_snippets(ingested)
    pass


def split_text_to_snippets(ingested_data):
    nlp = spacy.blank("da")
    nlp.add_pipe("sentencizer")

    snippets = []
    for webpage in ingested_data:
        doc = nlp(webpage.text)
        snippets.extend([Snippet(url=webpage.url, text=sent.text, index=i) for i, sent in enumerate(doc.sents)])
    return snippets    
    
