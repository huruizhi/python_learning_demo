from datetime import datetime
from elasticsearch5 import Elasticsearch
from pprint import pprint
es = Elasticsearch(hosts='elasticsearch-logging.logging.svc.cluster.local')


index = "test"

query = {"query": {"match_all": {}}}

doc_type = "20170103"


def main():
    es.indices.create(index=index, body={"mappings": {doc_type: {"properties": {"name": {"type": "text"}, "gender":{"type": "text"}, "age":{"type": "integer"}, "phone":{"type": "keyword"}}}}})

    es.index(index=index, doc_type='javalog', body={})

    res = es.search(index=index, body={"query": {"match_all": {}}})
    pprint(res)


def _test():
    resp = es.search(index, body=query, scroll="1m", size=100)

    scroll_id = resp['_scroll_id']
    resp_docs = resp["hits"]["hits"]
    total = resp['hits']['total']
    count = len(resp_docs)
    datas = resp_docs
    while len(resp_docs) > 0:
        scroll_id = resp['_scroll_id']
        resp = es.scroll(scroll_id=scroll_id, scroll="1m")
        resp_docs = resp["hits"]["hits"]
        datas.extend(resp_docs)
        count += len(resp_docs)
        if count >= total:
            break
    print (len(datas))


if __name__ == '__main__':
    main()
