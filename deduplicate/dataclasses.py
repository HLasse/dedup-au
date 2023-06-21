from dataclasses import dataclass

@dataclass
class Webpage:
    url: str
    text: str

@dataclass
class Snippet:
    url: str
    text: str
    index: int
