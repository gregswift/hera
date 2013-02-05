import os
from urlparse import urlparse

from suds.client import Client
from suds.transport.http import HttpAuthenticated
from suds.xsd.doctor import ImportDoctor, Import

VERSION = '1.2'
WSDL_PATH = '/usr/share/zeus/wsdl'
DEFAULT_WSDL = 'System.Cache'
HEADERS = { 'Content-Type': 'text/xml' }
HEADERS_1_2 = { 'Content-Type': 'application/soap+xml' }

def cleanWSDLName(wsdl):
    """Strips leading path and .wsdl ext from path name for consistency"""
    return os.path.basename(wsdl.replace('.wsdl',''))

class HeraException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)

class Hera:

    def __init__(self, username, password, location, version=VERSION,
            wsdl="System.Cache", wsdl_path=WSDL_PATH):
        
        self.location = location
        self.version = version
        self._wsdl_path = wsdl_path
        self._wsdl_orig = wsdl
        self._wsdl = self._getWSDL(wsdl, wsdl_path, version)

        # Apparently Zeus's wsdl is broken and we have to jimmy this thing in
        # manually.  See https://fedorahosted.org/suds/ticket/220 for details.
        imp = Import('http://schemas.xmlsoap.org/soap/encoding/')

        ### I'm getting rid of this... don't think its needed anymore?
        # Also, you'll be happy to know that the zillion .wsdl files that Zeus
        # includes apparently have different targetNamespace's. So..
        # we are defaulting to 1.2 now, but 
        #imp.filter.add('http://soap.zeus.com/zxtm/1.2/')

        self._doctor = ImportDoctor(imp)

        self._transport = HttpAuthenticated(username=username, password=password)

        self._loadWSDL(self._wsdl)

    def _getWSDL(self, wsdl, path, version):
        wsdl = cleanWSDLName(wsdl)
        versioned_wsdl = '{0}_{1}'.format(wsdl, version.replace('.','_'))
        path = os.path.abspath(path)
        available = self.availableWSDLs(path)
        for w in (versioned_wsdl, wsdl):
            if w in available:
                # Sorry windows
                return 'file://{0}.wsdl'.format(os.path.join(path,w))
        raise HeraException, 'Unable to locate WSDL {0}'.format(wsdl)

    def availableWSDLs(self, path):
        available = []
        for wsdl in os.listdir(path):
            if os.path.isfile(os.path.join(path,wsdl)):
                available.append(cleanWSDLName(wsdl))
        return available

    def _loadWSDL(self, wsdl, version=None, doctor=None, transport=None, location=None):
        if version is None:
            version = self.version
        _headers = HEADERS
        if version == '1.2':
            from suds.bindings import binding
            binding.envns = ('SOAP-ENV', 'http://www.w3.org/2003/05/soap-envelope')
            _headers = HEADERS_1_2
        if doctor is None:
            doctor = self._doctor
        if location is None:
            location = self.location
        if transport is None:
            (username, password) = self._transport.credentials()
            transport = HttpAuthenticated(username=username, password=password)
        self.client = Client(wsdl, doctor=doctor, transport=transport,
                location=location, headers=_headers)
        self._headers = _headers
        self._doctor = doctor
        self._location = location
        self._transport = transport

    def loadWSDL(self, wsdl, wsdl_path=None, version=None):
        if wsdl == self._wsdl:
            return
        if wsdl_path is None:
            wsdl_path = self._wsdl_path
        if version is None:
            version = self.version
        new_wsdl = self._getWSDL(wsdl, wsdl_path, version)
        self._loadWSDL(new_wsdl)
        self._wsdl_path = wsdl_path
        self._wsdl = new_wsdl

    def getVirtualServerNames(self):
        """Returns list of Virtual Servers"""
        wsdl = 'VirtualServer'
        self.loadWSDL(wsdl)
        return self.client.service.getVirtualServerNames()

    def getEnabledVirtualServers(self):
        """Returns list of enabled Virtual servers"""
        wsdl = 'VirtualServer'
        self.loadWSDL(wsdl)
        return self.client.service.getEnabled()

    def getPoolNames(self):
        """Returns list of pools"""
        wsdl = 'Pool'
        self.loadWSDL(wsdl)
        return self.client.service.getPoolNames()

    def getNodes(self,pool=None):
        """Returns list of nodes in pool"""
        wsdl = 'Pool'
        self.loadWSDL(wsdl)
        return self.client.service.getNodes([pool])

    def getNodesConnectionCounts(self, nodes=[]):
        """Return number of active connections for nodes"""
        wsdl = 'Pool'
        self.loadWSDL(wsdl)
        return self.client.service.getNodesConnectionCounts(nodes)

    def getDisabledNodes(self, pool=None):
        """Returns list of disabled nodes from pool"""
        wsdl = 'Pool'
        self.loadWSDL(wsdl)
        return self.client.service.getDisabledNodes([pool])

    def setDisableNodes(self, pool=None, nodes=[]):
        """Set nodes to draining in pool"""
        wsdl = 'Pool'
        self.loadWSDL(wsdl)
        if sum(self.getNodesConnectionCounts(nodes).values()) != 0:
            raise HeraException, 'Refusing to disable node(s) with active connections'
        return self.client.service.setDrainingNodes([pool], [nodes])

    def getDrainingNodes(self,pool=None):
        """Returns list of draining nodes from pool"""
        wsdl = 'Pool'
        self.loadWSDL(wsdl)
        return self.client.service.getDrainingNodes([pool])

    def setDrainingNodes(self, pool=None, node=[]):
        """Set nodes to draining in pool"""
        wsdl = 'Pool'
        self.loadWSDL(wsdl)
        return self.client.service.setDrainingNodes([pool], [node])

    def getGlobalCacheInfo(self):
        """Returns a small object of statistics."""
        wsdl = 'System.Cache'
        self.loadWSDL(wsdl)
        return self.client.service.getGlobalCacheInfo()

    def flushAll(self):
        """Flushes everything in the system: all objects across all virtual
        servers."""
        wsdl = 'System.Cache'
        self.loadWSDL(wsdl)
        return self.client.service.clearWebCache()

    def flushObjectsByPattern(self, url, return_list=False):
        """Flush objects out of the cache.  This accepts simple wildcards (*)
        in the host and/or path.  If return_list is True we'll return a list of
        URLs that matched the pattern.  There is a performance hit when
        returning the list since we have to request it, build it, and return
        it.  """
        wsdl = 'System.Cache'
        self.loadWSDL(wsdl)
        if return_list:
            objects = self.getObjectsByPattern(url)

        o = urlparse(url)
        r = self.client.service.clearMatchingCacheContent(o.scheme,
                                                          o.netloc,
                                                          o.path)
        if return_list and objects:
            return ["%s://%s%s" % (o.protocol, o.host, o.path)
                    for o in objects]
        else:
            return []


    def getObjectByPattern(self, url):
        """A simple convenience function.  If you have a full URL and you want
        a single object back, this is the one."""
        return self.getObjectsByPattern(url, 1)

    def getObjectsByPattern(self, url, limit=None):
        wsdl = 'System.Cache'
        self.loadWSDL(wsdl)
        o = urlparse(url)
        r = self.client.service.getCacheContent(o.scheme, o.netloc,
                                                o.path, limit)

        if r.number_matching_items:
            return r.matching_items

if __name__ == '__main__':
    import sys
    from getpass import getpass, getuser

    location = raw_input('What is the URL for the load balancer? ')
    wsdl_path = raw_input('Where are your WSDL files stored? [{0}] '.format(WSDL_PATH))
    if not wsdl_path:
        wsdl_path = WSDL_PATH
    wsdl = raw_input('Which WSDL do you want to load? [{0}] '.format(DEFAULT_WSDL))
    if not wsdl:
        wsdl = DEFAULT_WSDL
    user = getuser()
    username = raw_input('What is your username? [{0}] '.format(user))
    if not username:
        username = user
    print 'Now we need to authenticate {0} on {1}'.format(username,location)
    password = getpass('What is your password? ')
    h = Hera(username, password, location, wsdl=wsdl, wsdl_path=wsdl_path)
    h.getGlobalCacheInfo()