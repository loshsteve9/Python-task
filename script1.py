from elasticsearch import Elasticsearch
from faker_schema.faker_schema import FakerSchema


API_URI = "http://127.0.0.1:9200"

es = Elasticsearch([API_URI])

# indices creation

es.indices.create(index='project_1', ignore=400)
es.indices.create(index='project_2', ignore=400)
es.indices.create(index='project_3', ignore=400)

# data ingestion

faker = FakerSchema()


def ingest_data(esclient, index, schema):
    for i in range(10):
        doc = faker.generate_fake(schema)
        res = esclient.index(index=index, id=index+"_"+str(i), body=doc)


schema = {'employee_id': 'uuid4', 'employee_name': 'name',
          'employee address': 'address', 'email_address': 'email'}
ingest_data(es, "project_1", schema)
ingest_data(es, "project_2", schema)
ingest_data(es, "project_3", schema)
