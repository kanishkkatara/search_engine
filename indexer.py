"""
indexer exposes a function inex_docs which if given a data file, 
processes every line of that file, builds an inverted index from unigrams to a list of Document objects.
"""

from collections import Counter
from typing import List, Dict, Tuple

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

from index_interface import TermInDocument, SearchResult


def get_title_and_body(content: str) -> Tuple[str, str]:
    title, body = content.split("  ", 1)
    # remove leading quote in title
    if title[0] == '"':
        title = title[1:].lstrip()
    # remove trailing quote in body
    if body[-1] == '"':
        body = body[:-1].rstrip()
    # restrict title to at most 10 words
    if len(title) > 10:
        title = " ".join(title.split(" ")[:10])
    return title, body


def upload(collection: Collection, filename: str) -> int:
    # uploads articles from file into the mongodb collection and returns a count
    with open(filename) as fp:
        cnt = 0
        for line in fp:
            if len(line) == 0:
                continue
            fields = line.split(",", 1)
            docid = fields[0].strip()
            content = fields[1].strip()
            title, body = get_title_and_body(content)
            document = {"_id": docid, "title": title, "body": body}
            try:
                collection.insert_one(document)
            except DuplicateKeyError:
                collection.update_one({"_id": docid}, {"$set": {"title": title, "body": body}})
            cnt += 1
    return cnt


# Processing:
# 1. Find unigrams in content.
# 2. Remove stop words
# 3. Compute the position fraction of each unigram in the document. This is 1 if the it is the
# first word and close to 0 if it is the last word.
# 4. Compute the frequency of each unigram in the document
# 5. For each every unigram maintain a list of documents that the unigram can be found in.
# 5b. In addition to the docid, keep other metadata like the frequency of the unigram in
#     the document, the position of the unigram in the document.
def create_unigram_index(articles: Collection, unigrams: Collection) -> Tuple[int, int]:
    """
    Create a unigram TF index from the aricle collection and store in unigram collection
    Returns the number of unigrams and documents
    """
    stop_words = set(stopwords.words("english"))
    ps = PorterStemmer()
    posting_list = {}
    doccnt = 0
    for doc in articles.find():
        doccnt += 1
        content = doc["title"] + " " + doc["body"]
        word_tokens = [
            ps.stem(x.lower()) for x in word_tokenize(content) if x not in stop_words
        ]
        doclen = len(content)  # will be used to compute position feature
        wcount = 0
        for unigram in word_tokens:
            wcount += 1
            # process unigram
            if unigram not in posting_list:
                posting_list[unigram] = []
            if (len(posting_list[unigram]) == 0) or (
                posting_list[unigram][-1].docid != doc["_id"]
            ):
                # first occurrence of word/unigram in document
                posting_list[unigram].append(
                    TermInDocument(doc["_id"], doc["title"], 1, float(doclen - wcount) / doclen, 0.0)
                )
            else:
                posting_list[unigram][-1].term_freq += 1
    # Now we have processed all docs
    # Compute the score for all documents of all unigrams
    # Sort all posting lists based on score.
    # weights to be learned
    w_tf = 0.3
    w_pos = 0.7
    unicnt = 0
    for unigram in posting_list:
        unicnt += 1
        for doc in posting_list[unigram]:
            doc.score = w_tf * doc.term_freq + w_pos * doc.position
        posting_list[unigram] = sorted(posting_list[unigram], key=lambda x: x.score, reverse=True)
        unigrams.insert_one(
            {
                "_id": unigram,
                "docs": [
                    {
                        "docid": doc.docid,
                        "title": doc.title,
                        "term_freq": doc.term_freq,
                        "position": doc.position,
                        "score": doc.score,
                    }
                    for doc in posting_list[unigram]
                ],
            }
        )
    return (unicnt, doccnt)


def search_unigrams(expr: str, offset: int, limit: int, unigrams: Collection) -> List[SearchResult]:
    stop_words = set(stopwords.words("english"))
    ps = PorterStemmer()
    # Convert to lowercase and also stem the words
    search_words = [ps.stem(x.lower()) for x in word_tokenize(expr)]
    # Remove stop words from the set of words to search
    search_words = [x for x in search_words if x not in stop_words]
    # Add the score for each unigram
    doc_scores = Counter()
    docid_to_title = {}
    docid_to_position = {}
    for word in search_words:
        result = unigrams.find_one({"_id": word})
        if result is not None:
            for doc in result["docs"]:
                # Score of a document is the sum of scores of the document over all searched terms
                doc_scores[doc["docid"]] += doc["score"]
                docid_to_title[doc["docid"]] = doc["title"]
                docid_to_position[doc["docid"]] = doc["position"]
    final_results = [SearchResult(docid, docid_to_title[docid], score, docid_to_position[docid],) for docid, score in doc_scores.most_common(offset+limit)]
    return final_results[offset:offset+limit]
