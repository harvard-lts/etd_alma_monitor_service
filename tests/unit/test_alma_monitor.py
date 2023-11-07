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
        mock_response = """<searchRetrieveResponse xmlns="http://www.loc.gov/zing/srw/">
            <script/>
            <version>1.2</version>
            <numberOfRecords>0</numberOfRecords>
            <records/>
            <extraResponseData xmlns:xb="http://www.exlibris.com/repository/search/xmlbeans/">
            <xb:exact>true</xb:exact>
            <xb:responseDate>2023-11-07T13:22:41-0500</xb:responseDate>
            </extraResponseData>
            </searchRetrieveResponse>"""
        num_records = alma_monitor.get_number_alma_records(mock_response)
        assert num_records == 0