import logging
import xml.etree.ElementTree as ET
import os
import os.path


logger = logging.getLogger('etd_alma_monitor')
namespaces = {'mets': 'http://www.loc.gov/METS/',
              'xlink': 'http://www.w3.org/1999/xlink',
              'dim': 'http://www.dspace.org/xmlns/dspace/dim'}


class MetsExtractor:

    def __init__(self, mets_path=None) -> None: # pragma: no cover, 'set_mets_data' allows for unit testing # noqa
        if mets_path is not None:
            tree = ET.parse(mets_path)
            self.root = tree.getroot()
        else:
            self.root = None
        self.fileSec = {}
        self.degree_date = None
        self.identifier = None

    def set_mets_data(self, mets_data):
        self.root = ET.fromstring(mets_data)

    def get_amdid_and_mimetype(self, filename):
        '''Get the amdid and mimetype from the fileSec'''
        if self.root is None:
            raise Exception("MetsExtractor not initialized with mets data")
        # Get the file elements
        files = self.root.findall(".//mets:file", namespaces)
        for file in files:
            # Get the flocat element for the file
            flocat = file.find(".//mets:FLocat", namespaces)
            href = flocat.get("{" + namespaces['xlink'] + "}href")

            # If the href attribute matches the filename,
            # return the amdid and mimetype
            if href and href == os.path.basename(filename):
                amdid = file.get("ADMID")
                mimetype = file.get("MIMETYPE")
                if amdid and mimetype:
                    fileSecInfo = FileSecInfo(amdid, mimetype)
                    self.fileSec[filename] = fileSecInfo
                    return fileSecInfo
            elif href:
                # Try to get rid of special characters and see if they match
                href = href.encode('ascii', "ignore").decode('ascii')
                basefilename = os.path.basename(filename) \
                    .encode('ascii', "ignore").decode('ascii')
                if href == basefilename:
                    amdid = file.get("ADMID")
                    mimetype = file.get("MIMETYPE")
                    if amdid and mimetype:
                        fileSecInfo = FileSecInfo(amdid, mimetype)
                        self.fileSec[filename] = fileSecInfo
                        return fileSecInfo
        return FileSecInfo()

    def get_degree_date(self):
        '''Get the degree date from
        <dim:field mdschema="thesis" element="degree" qualifier="date">'''
        if self.degree_date is None:
            if self.root is None:
                raise Exception("MetsExtractor not initialized with mets data")
            field = self.root.find(".//dim:field[@mdschema='thesis'][@element='degree'][@qualifier='date']", namespaces)  # noqa
            if field is not None:
                self.degree_date = field.text
        return self.degree_date

    def get_dash_id(self):
        '''Get the identifier from
        <dim:field mdschema="dc" element="identifier" qualifier="other">'''
        if self.identifier is None:
            if self.root is None:
                raise Exception("MetsExtractor not initialized with mets data")
            field = self.root.find(".//dim:field[@mdschema='dc'][@element='identifier'][@qualifier='other']", namespaces)  # noqa
            if field is not None:
                self.identifier = field.text
        return self.identifier


class FileSecInfo:
    def __init__(self, amdid=None, mimetype=None):
        self.amdid = amdid
        self.mimetype = mimetype
