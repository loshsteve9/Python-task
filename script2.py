from elasticsearch import Elasticsearch
from time import sleep, time
import requests
import json

API_URI = "http://127.0.0.1:9200"


def get_indices(uri):
    """
    The purpose of this method is to extract indices with
    wildcard project_*
    """
    text = requests.get(uri+'/_cat/indices').text
    lines = text.split('\n')
    r = []
    for line in lines:
        line = [e.strip() for e in line.split(' ')]
        if len(line) > 2:
            if line[2].startswith('project_'):
                yield line[2]


tasks = []
for index in get_indices(API_URI):
    data = {"source": {"index": index},
            "dest": {"index": "project"}}
    r = requests.post(API_URI+"/_reindex?wait_for_completion=false", json=data)
    tasks.append(json.loads(r.text)['task'])

# wait for reindex tasks to complete
r = requests.get(API_URI+"/_tasks?actions=*reindex&wait_for_completion=true")
# add small sleeping here
sleep(1)
# add the last_update field for all documents
r = requests.post(API_URI+"/project/_update_by_query",
                  json={"query": {"match_all": {}},
                        "script": {"inline": "ctx._source.last_update = \"{}\"".
                                             format(time()), "lang": "painless"}})

print(r.text)
