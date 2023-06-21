from pathlib import Path

from tests.test_ingest import Webpage


def ingest_file(path: Path) -> list[Webpage]:
    with path.open("r") as f:
        lines = f.readlines()
        
    output_webpages  = []
    url = None
    
    for line in lines:
        if line.startswith("***"):
            chunks = line.split(" ")
            url = [chunk for chunk in chunks if chunk.startswith("http")][0]
        elif len(line) > 2:
            if url is None:
                raise ValueError("URL not set")
            output_webpages.append(Webpage(url=url, text=line))
            url = None
            
    return output_webpages