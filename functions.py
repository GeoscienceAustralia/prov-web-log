import urllib
import json
import requests
from lxml import etree
import settings
import string
import random
from datetime import datetime
import os
import tempfile
from rdflib import Graph, URIRef, Namespace, RDF, RDFS, XSD, Literal
from itertools import izip_longest


def get_all_service_metadata_records(xml_file, proxy=False):
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

    if proxy:
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
    else:
        r = requests.post('http://ecat.ga.gov.au/geonetwork/srv/eng/csw',
                          data=q,
                          headers={'Content-Type': 'application/xml'})
        with open(xml_file, 'w') as f:
            f.write(r.content)

    return True


def generate_geocatid_endpoint_pairs_json_file(xml_file, json_file):
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


def get_service_geocatid(pairs_dict, service_endpoint):
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


def create_entity_triples(output_entity_uri, label, activity_uri, process_start_time):
    PROV = Namespace("http://www.w3.org/ns/prov#")
    g = Graph()
    e = URIRef(output_entity_uri)
    g.add((e, RDF.type, PROV.Entity))
    g.add((e, RDFS.label, Literal(label, datatype=XSD.string)))
    g.add((e, PROV.generatedAtTime, Literal(datetime.strftime(process_start_time, '%Y-%m-%dT%H:%M:%S.%f'), datatype=XSD.datetime)))
    g.add((e, PROV.wasGeneratedBy, URIRef(activity_uri)))

    return g


def triplify_line(process_start_time, server_ip, http_verb, request_url, request_qsa, client_ip, pairs_dict):
    geocatid = get_service_geocatid(pairs_dict, request_url)
    if geocatid is not None:
        # make the Web Service Agent URI
        service_uri = settings.BASE_URI_SERVICE + geocatid
        # make the client Agent URI
        client_uri = settings.BASE_URI_CLIENT + client_ip
        # make the Activity URI
        activity_uri = settings.BASE_URI_ACTIVITY + id_generator()
        # make the procedure (Entity) URI
        procedure_uri = settings.BASE_URI_ENTITY + id_generator()
        # make the output entity (Entity) URI
        output_entity_uri = settings.BASE_URI_ENTITY + id_generator()

        g = create_client_triples(client_uri)
        g += create_service_triples(service_uri, client_uri)
        g += create_procedure_triples(procedure_uri, request_qsa, client_uri)
        g += create_activity_triples(activity_uri, process_start_time, service_uri, procedure_uri)
        # TODO: improve the label
        g += create_entity_triples(output_entity_uri, 'Web Service Request Output', activity_uri, process_start_time)

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
            if len(words) >= 7:
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


def db_insert(graph_file):
    """ Make a non-secure insert into the DB
    """
    # SPARQL INSERT
    data = {'update': 'INSERT DATA { ' + open(graph_file).read() + ' }'}
    r = requests.post(settings.FUSEKI_UPDATE_URI, data=data)
    try:
        if r.status_code != 200 and r.status_code != 201:
            return [False, r.text]
        return [True, r.text]
    except Exception, e:
        print e.message
        return [False, e.message]


def grouper(n, iterable, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


def split_graph_file(graph_file):
    n = 5000
    with open(graph_file) as f:
        for i, g in enumerate(grouper(n, f, fillvalue=None)):
            with tempfile.NamedTemporaryFile('w', delete=False) as fout:
                for j, line in enumerate(g, 1):  # count number of lines in group
                    if line is None:
                        j -= 1  # don't count this line
                        break
                    fout.write(line)
            subgraph_name = 'subgraph_{0}'.format(i * n + j) + '.nt'
            os.rename(fout.name, subgraph_name)


def load_split_log(dir):
    for f in os.listdir(dir):
        if f.startswith('subgraph_'):
            db_insert(f)


def tidy_up(dir, graph_file):
    os.unlink(graph_file)
    for f in os.listdir(dir):
        if f.startswith('subgraph_'):
            os.unlink(f)


if __name__ == '__main__':
    #split_graph_file('graph.nt')
    #load_split_log('.')
    tidy_up('.', 'graph.nt')