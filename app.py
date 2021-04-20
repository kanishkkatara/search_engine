from flask import Flask, request
from flask_cors import CORS
from urllib.parse import unquote_plus
from pymongo import MongoClient

import indexer

app = Flask(__name__)
CORS(app)  # to allow the web application to access this app
appdb = MongoClient("mongodb://root:Secret@mongo").appdb
app.logger.info("initialized flask, CORS, and MongoClient")


@app.route("/upload_data")
def upload_articles():
    app.logger.info("upload_articles")
    # upload all the articles from the data directory into a new articles collection
    num = indexer.upload(appdb.articles, "data/top_500_lateral_wiki_utf8.csv")
    return {"count": num}


@app.route("/lookup/<string:article_id>")
def lookup(article_id):
    app.logger.info(f'lookup {article_id}')
    try:
        return appdb.articles.find({"_id": article_id}).next()
    except StopIteration:
        return {}


@app.route("/create_index")
def create_index():
    app.logger.info("started create_index")
    # index all the documents in mongodb
    unicnt, doccnt = indexer.create_unigram_index(appdb.articles, appdb.unigrams)
    app.logger.info("finished create_index")
    return {"unigram count": unicnt, "document count": doccnt}


@app.route("/search", methods=['GET'])
def search():
    searched_expression = request.args.get('q', "")
    offset = int(request.args.get('offset', 0))
    limit = int(request.args.get('limit', 10))
    expr = unquote_plus(searched_expression)
    app.logger.info(f"search expr {expr} offset {offset} limit {limit} unigrams {appdb.unigrams}")
    results = indexer.search_unigrams(expr, offset, limit, appdb.unigrams)
    return {"results": [{"_id": res.docid, "title:": res.title, "score": res.score, "position": res.position} for res in results]}
