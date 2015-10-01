import os
import sys
import uuid
from arango import Arango

db_name = 'fstest'
col_name = 'filesystem'
arango = Arango(host="localhost", port=8529)

try:
    arango.delete_database(db_name)
except:
    pass

db = arango.create_database(db_name)
graph = db.create_graph('filesystem')
fsnodes = db.create_collection('fsnodes')
graph.create_vertex_collection('fsnodes')

db.create_collection('contains', is_edge=True)

graph.create_edge_definition(
        edge_collection='contains',
        from_vertex_collections=['fsnodes'],
        to_vertex_collections=['fsnodes'])


for dirname, dirnames, filenames in os.walk(sys.argv[1]):
    key = dirname.replace('/', '_')
    d = dict(type='dir', dirname=dirname, _key=key)
    graph.create_vertex('fsnodes', d)
    for fname in filenames:
        full_filename=filename = os.path.join(dirname, fname)
        print filename
        size = os.path.getsize(filename)
        print size
        filename = filename.replace('/', '_').replace(' ', '').replace('(', '').replace(')', '')
        print filename
        f = dict(type='file', filename=full_filename, _key=filename, size=size)
        graph.create_vertex('fsnodes', f)
        graph.create_edge('contains',
                dict(_from='fsnodes/'+d['_key'], _to='fsnodes/'+f['_key']))



#arango.delete_database(db_name)
