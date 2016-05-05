import urllib
import json
import requests
from lxml import etree
import settings
import string
import random
from datetime import datetime
from rdflib import Graph, URIRef, Namespace, RDF, RDFS, XSD, Literal


def get_all_service_metadata_records(xml_file):
    q = '''
    <csw:GetRecords xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" xmlns:gmd="http://www.isotc211.org/2005/gmd" service="CSW" version="2.0.2"
        resultType="results" outputSchema="own"
        maxRecords="1000"
        startPosition="1">
        <csw:Query typeNames="csw:Record">
            <csw:Constraint version="1.1.0">
                <Filter xmlns="http://www.opengis.net/ogc" xmlns:gml="http://www.opengis.net/gml">
                    <PropertyIsEqualTo>
                        <PropertyName>type</PropertyName>
                        <Literal>service</Literal>
                    </PropertyIsEqualTo>
                </Filter>
            </csw:Constraint>
        </csw:Query>
    </csw:GetRecords>
    '''

    creds = json.load(open('creds.json'))
    ga_proxy = {
        'http': 'http://' + creds['usr'] + ':' + creds['pwd'] + '@' + creds['ga_proxy'],
        'https': 'https://' + creds['usr'] + ':' + creds['pwd'] + '@' + creds['ga_proxy'],
    }
    r = requests.post('http://ecat.ga.gov.au/geonetwork/srv/eng/csw',
                      data=q,
                      headers={
                          'Content-Type': 'application/xml'
                      },
                      proxies=ga_proxy)
    with open(xml_file, 'w') as f:
        f.write(r.content)

    return True


def generate_uuid_endpoint_pairs_json_file(xml_file, json_file):
    pairs = {}
    xml = etree.parse(xml_file)
    records = xml.xpath('///mdb:MD_Metadata/mdb:identificationInfo/srv:SV_ServiceIdentification/mri:citation/cit:CI_Citation/cit:identifier/mcc:MD_Identifier/mcc:code/gco:CharacterString/text() | '+
                        '//mdb:MD_Metadata/mdb:distributionInfo/mrd:MD_Distribution/mrd:transferOptions/mrd:MD_DigitalTransferOptions/mrd:onLine/cit:CI_OnlineResource/cit:linkage/gco:CharacterString/text()',
                  namespaces={
                      'mdb': 'http://standards.iso.org/iso/19115/-3/mdb/1.0',
                      'mcc': 'http://standards.iso.org/iso/19115/-3/mcc/1.0',
                      'gco': 'http://standards.iso.org/iso/19115/-3/gco/1.0',
                      'mrd': 'http://standards.iso.org/iso/19115/-3/mrd/1.0',
                      'cit': 'http://standards.iso.org/iso/19115/-3/cit/1.0',
                      'srv': 'http://standards.iso.org/iso/19115/-3/srv/2.0',
                      'mri': 'http://standards.iso.org/iso/19115/-3/mri/1.0',
                      'csw': 'http://www.opengis.net/cat/csw/2.0.2'
                  })

    uuid = None
    for x in records:
        print x
        if not (x.startswith('http') or x.startswith('Product')):
            uuid = x
        else:
            if uuid is not None:
                # don't store the QSAs, only the service URI
                uri = x.split('?')[0]
                uri = uri.replace('http://www.ga.gov.au', '')\
                         .replace('http://services.ga.gov.au', '')\
                         .replace('http://intranet.ga.gov.au', '')\
                         .replace('http://intranet-test.ga.gov.au', '')\
                         .replace('http://np.ga.gov.au', '')\
                         .replace('http://eos.ga.gov.au', '')\
                         .replace('http://ga.gov.au', '')
                pairs[urllib.quote_plus(uri)] = uuid

    with open(json_file, 'w') as j:
        j.write(json.dumps(pairs))

    return True


def get_service_uuid(pairs_dict, service_endpoint):
    try:
        geocatid = pairs_dict[urllib.quote_plus(service_endpoint)]
    except KeyError:
        geocatid = None

    return geocatid


def id_generator(size=6, chars=string.ascii_lowercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def create_client_triples(client_uri):
    """

    :rtype: Graph()
    """
    PROV = Namespace("http://www.w3.org/ns/prov#")
    g = Graph()
    a = URIRef(client_uri)
    g.add((a, RDF.type, PROV.Agent))
    g.add((a, RDFS.label, Literal('Web Service client', datatype=XSD.string)))

    return g


def create_service_triples(service_uri, client_uri):
    PROV = Namespace("http://www.w3.org/ns/prov#")
    g = Graph()
    a = URIRef(service_uri)
    g.add((a, RDF.type, PROV.Agent))
    g.add((a, PROV.actedOnBehalfOf, URIRef(client_uri)))

    return g


def create_procedure_triples(procedure_uri, request_qsa, client_uri):
    PROV = Namespace("http://www.w3.org/ns/prov#")
    g = Graph()
    a = URIRef(procedure_uri)
    g.add((a, RDF.type, PROV.Plan))
    g.add((a, RDFS.label, Literal('Web Service request', datatype=XSD.string)))
    g.add((a, PROV.value, Literal(request_qsa, datatype=XSD.string)))
    g.add((a, PROV.wasAttributedTo, URIRef(client_uri)))

    return g


def create_activity_triples(activity_uri, process_start_time, service_uri, procedure_uri):
    PROV = Namespace("http://www.w3.org/ns/prov#")
    g = Graph()
    a = URIRef(activity_uri)
    g.add((a, RDF.type, PROV.Activity))
    g.add((a, RDFS.label, Literal('Web Service call', datatype=XSD.string)))
    g.add((a, PROV.startedAtTime, Literal(datetime.strftime(process_start_time, '%Y-%m-%dT%H:%M:%S.%f'), datatype=XSD.datetime)))
    g.add((a, PROV.wasAssociatedWith, URIRef(service_uri)))
    g.add((a, PROV.used, URIRef(procedure_uri)))

    return g


def triplify_line(process_start_time, server_ip, http_verb, request_url, request_qsa, client_ip, pairs_dict):
    geocatid = get_service_uuid(pairs_dict, request_url)
    if geocatid is not None:
        # make the Web Service Agent URI
        service_uri = settings.BASE_URI_SERVICE + geocatid
        # make the client Agent URI
        client_uri = settings.BASE_URI_CLIENT + client_ip
        # make the Activity URI
        activity_uri = settings.BASE_URI_ACTIVITY + id_generator()
        # make the procedure (Entity) URI
        procedure_uri = settings.BASE_URI_ENTITY + id_generator()

        g = create_client_triples(client_uri)
        g += create_service_triples(service_uri, client_uri)
        g += create_procedure_triples(procedure_uri, request_qsa, client_uri)
        g += create_activity_triples(activity_uri, process_start_time, service_uri, procedure_uri)

        return g
    else:
        return None


def process_log(log_file, pairs_dict):
    g = Graph()
    with open(log_file) as f:
        lines = f.read().split('\n')

    for line in lines:
        # ignore lines starting with hash
        if line.startswith('#'):
            pass
        else:
            # 0 2016-04-25
            # 1 00:00:00.911
            # 2 192.104.43.119
            # 3 GET
            # 4 /gis/services/Geological_Provinces_2013/MapServer/WMSServer
            # 5 &SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&FORMAT=image/png&WIDTH=900&CRS=EPSG:4326&HEIGHT=637&LAYERS=TectonicProvinces-Proterozoic&STYLES=&BBOX=-49.86143,106.569060975,-3.62699999999995,171.88106 80
            # 6 -
            # 7 192.104.43.57
            words = line.split(' ')
            process_start_date = datetime.strptime(words[0] + 'T' + words[1], '%Y-%m-%dT%H:%M:%S.%f')
            server_ip = words[2]
            http_verb = words[3]
            request_url = words[4]
            request_qsa = words[5]
            #port = words[6]
            #dash = words[6]
            client_ip = words[8]

            g1 = triplify_line(process_start_date, server_ip, http_verb, request_url, request_qsa, client_ip, pairs_dict)
            if g1 is not None:
                g += g1

    return g


def query_log_graph(graph_file):
    g = Graph().parse(graph_file, format='turtle')

    q = '''
    PREFIX prov: <http://www.w3.org/ns/prov#>
    SELECT (COUNT(?a) AS ?cnt)
    WHERE {
        ?a a prov:Activity .
    }
    '''

    for r in g.query(q):
        print r
    return None


def db_insert(turtle, from_string=False):
    """ Make a non-secure insert into the DB
    """
    #convert the Turtle into N-Triples
    g = Graph()
    if from_string:
        g.parse(data=turtle, format='text/turtle')
    else:
        g.load(turtle, format='n3')

    # SPARQL INSERT
    data = {'update': 'INSERT DATA { ' + g.serialize(format='nt') + ' }'}
    r = requests.post(settings.FUSEKI_UPDATE_URI, data=data)
    try:
        if r.status_code != 200 and r.status_code != 201:
            return [False, r.text]
        return [True, r.text]
    except Exception, e:
        print e.message
        return [False, e.message]



xml_file = 'results.xml'
json_file = 'pairs.json'
log_file = 'weblog.txt'
graph_file = 'graph.ttl'
#get_all_service_metadata_records(xml_file)
#generate_uuid_endpoint_pairs_json_file(xml_file, json_file)
#pairs_dict = json.load(open(json_file, 'r'))
#uuid = get_service_uuid(pairs_dict, '/gis/rest/services/climate/PSMACadLiteEasements/MapServer')
#g = process_log(log_file, pairs_dict)

#with open(graph_file, 'w') as gf:
#    gf.write(g.serialize(format='turtle'))

#query_log_graph(graph_file)

db_insert('graph.ttl')
