# switch on command line args
import sys
import functions
import json


services_xml = 'services.xml'
pairs_json = 'pairs.json'
graph_file = 'graph.nt'
g = None

if sys.argv[1] == 'get':
    # get the XML for all services
    functions.get_all_service_metadata_records(services_xml)
elif sys.argv[1] == 'process':
    # process the KVPs for services and endpoints out of the XML
    functions.generate_geocatid_endpoint_pairs_json_file(services_xml, pairs_json)
elif sys.argv[1] == 'triplify':
    # triplify a log file
    g = functions.process_log(sys.argv[2], json.load(open(pairs_json, 'r')))
    with open(graph_file, 'w') as f:
        f.write(g.serialize(format='nt'))
elif sys.argv[1] == 'send':
    # send triples to DB
    r = functions.db_insert(graph_file)
    #os.unlink(graph_file)
