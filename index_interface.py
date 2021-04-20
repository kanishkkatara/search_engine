from dataclasses import dataclass

@dataclass
class TermInDocument:
    '''
    Count the occurrence of a term in a document
    '''
    docid: str
    title: str
    term_freq: int = 0
    position: float = 0.0
    score: float = 0.0

@dataclass
class SearchResult:
    """SearchResult is a struct with information that is returned to the user"""

    docid: str
    title: str
    score: float = 0.0
    position: float = 0.0
