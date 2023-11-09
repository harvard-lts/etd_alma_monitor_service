import unittest
from unittest.mock import patch
from etd.alma_monitor import AlmaMonitor
import pytest


class TestAlmaMonitor(unittest.TestCase):

    @patch('etd.alma_monitor.MongoUtil')
    def test_poll_for_alma_submissions(self, MockMongoUtil):
        alma_monitor = AlmaMonitor()
        # Mock the query_records method to return mock data
        mock_mongo_util = MockMongoUtil.return_value

        # Set up a mock result for the query_records method
        mock_mongo_util.query_records.return_value = [
            {'_id': '6545889182013d2d2b1a77c5', 'proquest_id': 1234567,
             'school_alma_dropbox': 'gsd',
             'alma_submission_status': 'ALMA_DROPBOX',
             'directory_id': "proquest1234-5678-gsd"},
            {'_id': '6545889182013d2d2b1a77c6', 'proquest_id': 2345678,
             'school_alma_dropbox': 'dce', 'alma_submission_status': 'ALMA',
             'directory_id': "proquest1234-5678-dce"}]

        # Call the function to be tested
        result = alma_monitor.poll_for_alma_submissions()

        # Assert the result or perform any required assertions
        assert result[0]['proquest_id'] == 1234567
        assert result[0]['school_alma_dropbox'] == 'gsd'
        assert result[0]['alma_submission_status'] == 'ALMA_DROPBOX'
        assert result[0]['directory_id'] == "proquest1234-5678-gsd"
        assert result[1]['proquest_id'] == 2345678
        assert result[1]['school_alma_dropbox'] == 'dce'
        assert result[1]['alma_submission_status'] == 'ALMA'
        assert result[1]['directory_id'] == "proquest1234-5678-dce"

        def raise_exception(*args, **kwargs):
            raise Exception("Test exception")

        mock_mongo_util.query_records.side_effect = raise_exception

        alma_monitor = AlmaMonitor()
        alma_monitor.mongo_util = mock_mongo_util

        with pytest.raises(Exception) as exc_info:
            alma_monitor.poll_for_alma_submissions()

        # Assert the result or perform any required assertions
        assert "Test exception" in str(exc_info.value)

    def test_get_number_alma_records(self):
        alma_monitor = AlmaMonitor()
        # Mock the query_records method to return mock data
        mock_response = """<searchRetrieveResponse
            xmlns="http://www.loc.gov/zing/srw/">
            <script/>
            <version>1.2</version>
            <numberOfRecords>1</numberOfRecords>
            <records/>
            <extraResponseData
            xmlns:xb="http://www.exlibris.com/repository/search/xmlbeans/">
            <xb:exact>true</xb:exact>
            <xb:responseDate>2023-11-07T13:22:41-0500</xb:responseDate>
            </extraResponseData>
            </searchRetrieveResponse>"""
        num_records = alma_monitor.get_number_alma_records(mock_response)
        assert num_records == 1

    def test_get_alma_id(self):
        # Mock response truncated for easier reading
        mock_response = """<searchRetrieveResponse
        xmlns="http://www.loc.gov/zing/srw/">
        <script/>
        <version>1.2</version>
        <numberOfRecords>1</numberOfRecords>
        <records>
            <record>
            <recordSchema>mods</recordSchema>
            <recordPacking>xml</recordPacking>
            <recordData>
            <mods xmlns="http://www.loc.gov/mods/v3"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            version="3.5" xsi:schemaLocation="http://www.loc.gov/mods/v3
            http://www.loc.gov/standards/mods/v3/mods-3-5.xsd">
            <recordInfo>
                <descriptionStandard>rda</descriptionStandard>
                <recordContentSource authority="marcorg">MH
                </recordContentSource>
                <recordCreationDate encoding="marc">231106
                </recordCreationDate>
                <recordIdentifier>99156845176203941</recordIdentifier>
                <recordOrigin>Converted from MARCXML to MODS version
                3.5 using MARC21slim2MODS3-5.xsl (Revision 1.112 
                2018/06/21)</recordOrigin>
                <languageOfCataloging>
                <languageTerm authority="iso639-2b" type="code">eng
                </languageTerm>
                </languageOfCataloging>
            </recordInfo>
            </mods>
            </recordData>
            <recordIdentifier>99156845176203941</recordIdentifier>
            <recordPosition>1</recordPosition>
            </record>
        </records>
        <extraResponseData xmlns:xb=
        "http://www.exlibris.com/repository/search/xmlbeans/">
        <xb:exact>true</xb:exact>
        <xb:responseDate>2023-11-08T14:44:10-0500</xb:responseDate>
        </extraResponseData>
        </searchRetrieveResponse>"""
        
        alma_monitor = AlmaMonitor()
        record_id = alma_monitor.get_alma_record_id(mock_response)
        assert record_id == "99156845176203941"

    def test_invoke_dims(self):
        record = {'_id': '6545889182013d2d2b1a77c5', 'proquest_id': "1234567",
             'school_alma_dropbox': 'gsd',
             'alma_submission_status': 'ALMA_DROPBOX',
             'directory_id': "proquest1234-5678-gsd",
             "unit_testing": True}
    
        alma_monitor = AlmaMonitor()
        dims_json = alma_monitor.invoke_dims(record, "99156845176203941")
        assert dims_json["package_id"] == "proquest1234-5678-gsd"
        assert dims_json["depositing_application"] == "ETD"
        assert dims_json["admin_metadata"]["ownerCode"] == "GSD.LIBR"
        assert dims_json["admin_metadata"]["billingCode"] == "GSD.LIBR.Theses_0001"
        assert dims_json["admin_metadata"]["urnAuthorityPath"] == "GSD.LOEB"
        assert dims_json["admin_metadata"]["pq_id"] == "PQ-1234567"
        assert dims_json["admin_metadata"]["alma_id"] == "99156845176203941"
        
    
    def test_format_etd_osn(self):
        alma_monitor = AlmaMonitor()
        osn = alma_monitor.format_etd_osn("gsd", "thesis.pdf", "12345", "amd_primary", "2023")
        assert osn == "ETD_THESIS_gsd_2023_PQ_12345"
        mets_osn = alma_monitor.format_etd_osn("gsd", "mets.xml", "12345", None, "2023")
        assert mets_osn == "ETD_DOCUMENTATION_gsd_2023_PQ_12345"
        supp_osn = alma_monitor.format_etd_osn("gsd", "supp.pdf", "12345", "amd_supplemental", "2023")
        assert supp_osn == "ETD_THESIS_SUPPLEMENT_gsd_2023_PQ_12345"
        lic_osn = alma_monitor.format_etd_osn("gsd", "license.pdf", "12345", "amd_license", "2023")
        assert lic_osn == "ETD_LICENSE_gsd_2023_PQ_12345"
