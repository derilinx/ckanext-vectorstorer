
import requests
import xml.etree.ElementTree as ET

import logging
log = logging.getLogger(__name__)

NS = "{http://www.opengis.net/wms}"

def ns(s):
    return "%s%s" %(NS,s)

def wms_from_url(url):
    resp = requests.get(url)
    resp.encoding = 'utf-8'
    xml_doc = resp.text.encode('utf-8')

    root = ET.fromstring(xml_doc)

    return root

# umm.
def wms_from_string(s):
    return ET.fromstring(s)


# Structure is:
# WMS_Capabilities
#  Service
#  Capability
#   Request
#   Exception
#   Layer
#    CRS -- All supported Projections
#    EX_GeographicBoundingBox
#    Layer
#     CRS -- Native Projections
#     EX_GeographicBoundingBox
#     ...
#    Layer...


def layers(wms):
    return wms.findall('.//%s/%s' % (ns('Layer'), ns('Layer')))

def superLayer(wms):
    return wms.find('.//%s' % ns('Layer'))

def name_for_layer(layer):
    return layer.find(ns('Name')).text

def ex_geographicboundingbox(layer):
    bbox = layer.find(ns('EX_GeographicBoundingBox'))
    print list(bbox)

    return dict([(k,bbox.find(ns(k)).text) for k in ('westBoundLongitude', 'eastBoundLongitude',
                                                     'southBoundLatitude', 'northBoundLatitude')])

def crs_for_layer(layer):
    crs = layer.findall(ns('CRS'))
    return [elt.text for elt in crs]


# somewhat sketchy, as we're getting the bounding box from the superlayer, and the crs from the
# first layer.
def geo_metadata(wms):
    try:
        return {'EX_GeographicBoundingBox': ex_geographicboundingbox(superLayer(wms)),
                'crs': crs_for_layer(layers(wms)[0]) }
    except Exception as msg:
        log.error("Exception getting geo metadata for wms: %s" %msg)
        return {}
