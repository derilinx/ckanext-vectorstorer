import wms

with open('test/capabilities.xml', 'r') as f:
    wms_doc = wms.wms_from_string(f.read())

print wms.geo_metadata(wms_doc)

print wms.geo_metadata(wms.wms_from_url('https://data.odm-eu.staging.derilinx.com/geoserver/ODCambodia/Fishery_En/wms?service=WMS&request=GetCapabilities'))
