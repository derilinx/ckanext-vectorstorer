from urlparse import urlparse, urljoin

class WMSResource:
    name_extention=" Web Map Service in GeoServer"
    _get_capabilities_url="?service=WMS&request=GetCapabilities"
    _name= None
    _description= None
    _package_id= None
    _url= None
    _format= u'WMS'
    _parent_resource_id=None
    _wms_server= None
    _wms_layer= None
    _vectorstorer_resource= u'vectorstorer_wms'
    
    def __init__(self,package_id, name, description, parent_resource_id, wms_server,wms_layer):
	self._package_id=package_id
	self._name=name + self.name_extention
	self._description=description
	base_url = urlparse(wms_server)
        simple_url = urljoin( base_url.netloc,self._get_capabilities_url)
        simple_url_parts = simple_url.split('/')
        last_part = simple_url_parts[-1] #i.e. wms?service.... etc
        first_part = join(simple_url_parts[0:-1], '/')
        wms_parts = wms_layer.split(':')
        workspace = wms_parts[0]
        layer = wms_parts[1]
	#This is a filtered GetCapabilities URL so that CKAN etc. still gets what's expected but only this layer
        self._url=first_part + '/' + workspace + '/' + layer + '/' +  last_part
	self._parent_resource_id=parent_resource_id
	self._wms_server=wms_server
	self._wms_layer=wms_layer
	
	
    def get_as_dict(self):
	resource = {
	  "package_id":unicode(self._package_id),
	  "url":self._wms_server + self._get_capabilities_url,
	  "format":self._format,
	  "parent_resource_id":self._parent_resource_id,
	  'vectorstorer_resource': self._vectorstorer_resource,
	  "wms_server": self._wms_server,
	  "wms_layer": self._wms_layer,
	  "name":self._name,
	  "description": self._description }
	
	return resource
	
class DBTableResource:
    name_extention=" in Datastore via GeoServer"
    _name= None
    _description= None
    _package_id= None
    _url= None
    _format= 'DB_TABLE'
    _datastore_active=True
    _parent_resource_id=None
    _geometry= None
    _vectorstorer_resource= u'vectorstorer_db'
    
    def __init__(self,package_id, name, description, parent_resource_id, url, geometry):
	self._package_id=package_id
	self._name=name + self.name_extention
	self._description=description
	self._url=url
	self._parent_resource_id=parent_resource_id
	self._geometry=geometry
	
	
    def get_as_dict(self):
	resource = {
	  "package_id":unicode(self._package_id),
	  "url":self._url,
	  "format":self._format,
	  "parent_resource_id":self._parent_resource_id,
	  "geometry":self._geometry,
	  'vectorstorer_resource': self._vectorstorer_resource,
	  "datastore_active": self._datastore_active,
          "name":self._name,
	  "description": self._description }
	
	return resource
	
	
	
	
	
	
	
	
