import os.path
import os
from etd.mets_extractor import MetsExtractor


def test_mets_extractor():
    mets_test_file = os.path.join(os.getenv("ALMA_DATA_DIR",
                                            "/home/etdadm/tests/unit/data"),
                                            "mets.xml")
    mets_extractor = MetsExtractor(mets_test_file)
    amdid_thesis = mets_extractor.get_amdid_and_mimetype(
        "ES 100 Final Thesis PDF - Liam Nuttall.pdf")
    amdid_license = mets_extractor.get_amdid_and_mimetype(
        "Harvard_IR_License_-_LAA_for_ETDs_(2020).pdf")
    degree_date = mets_extractor.get_degree_date()
    dash_id = mets_extractor.get_dash_id()
    assert amdid_thesis.amdid == "amd_primary"
    assert amdid_license.amdid.startswith("amd_license")
    assert degree_date == "2022"
    assert dash_id == "29161227"
