from etd.mets_extractor import MetsExtractor
import pytest

# flake8: noqa
def test_mets_extractor():
    mets_mock_file = """<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<mets xmlns="http://www.loc.gov/METS/" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ID="etd-mets-1" OBJID="etd-submission-892650" TYPE="DSpace ITEM" PROFILE="http://www.dspace.org/schema/aip/mets_aip_1_0.xsd" xsi:schemaLocation="http://www.loc.gov/METS/ http://www.loc.gov/standards/mets/mets.xsd">
  <metsHdr CREATEDATE="2022-07-19T12:04:52-04:00">
    <agent ROLE="CREATOR" TYPE="OTHER">
      <name>ETD Administrator, ProQuest</name>
    </agent>
  </metsHdr>
  <dmdSec ID="etdadmin-mets-dmd-1" GROUPID="etdadmin-mets-dmd-1">
    <mdWrap LABEL="DIM Metadata" MDTYPE="OTHER" OTHERMDTYPE="DIM">
      <xmlData xmlns:dim="http://www.dspace.org/xmlns/dspace/dim">
        <dim:dim>
          <dim:field mdschema="dc" element="contributor" qualifier="author" authority="">Nuttall, Liam</dim:field>
          <dim:field mdschema="dc" element="identifier" qualifier="other">29161227</dim:field>
          <dim:field mdschema="dc" element="title">Atmospheric Water Capture in Arid, Rural Environments Using Solid Sorbents</dim:field>
          <dim:field mdschema="dc" element="description" qualifier="abstract">All around the world, there are still hundreds of millions of people who lack regular access to clean, safe drinking water. While there are many ways that various organizations attempt to aid this situation, one promising reservoir from which additional water could be generated is the atmosphere. The technology to collect this moisture exists for higher humidity values but does not exist for arid environments. This project aims to create a device that can pull water out of the air as a drinking source by using solid sorbent materials in an absorption and desorption cycle. It also must be able to operate in rural areas without external power sources, so it will take advantage of a direct conversion of solar to thermal energy. After several experiments and iterative design changes, the device was able to produce approximately 0.5 mL of clean drinking water per daily cycle. However, with additional development of certain components and at a larger scale, this device is a very promising solution for such a prevalent issue.</dim:field>
          <dim:field mdschema="dc" element="subject" qualifier="PQ">Environmental engineering</dim:field>
          <dim:field mdschema="dc" element="contributor" qualifier="advisor">Martin, Scot T</dim:field>
          <dim:field mdschema="dc" element="date" qualifier="created">2022</dim:field>
          <dim:field mdschema="dc" element="date" qualifier="submitted">2022</dim:field>
          <dim:field mdschema="dc" element="date" qualifier="issued">2022-06-03</dim:field>
          <dim:field mdschema="dc" element="format" qualifier="mimetype">application/pdf</dim:field>
          <dim:field mdschema="dc" element="language" qualifier="iso">en</dim:field>
          <dim:field mdschema="dc" element="type" qualifier="material">text</dim:field>
          <dim:field mdschema="dc" element="type">Thesis or Dissertation</dim:field>
          <dim:field mdschema="dc" element="description" qualifier="provenance">Submission originally under a zero month embargo, it is available immediately.</dim:field>
          <dim:field mdschema="dc" element="description" qualifier="provenance">The student, 2451669, accepted the attached license on 2022-04-14 at 17:28.</dim:field>
          <dim:field mdschema="dc" element="description" qualifier="provenance">The student, Nuttall, Liam, submitted this Dissertation for approval on 2022-04-14 at 17:31.</dim:field>
          <dim:field mdschema="dc" element="description" qualifier="provenance">2022-06-03</dim:field>
          <dim:field mdschema="dc" element="description" qualifier="provenance">DSpace AIP METS Submission Ingestion Package generated from ETD Administrator submission #892650 on 2022-07-19 at 12:04:52</dim:field>
          <dim:field mdschema="thesis" element="degree" qualifier="date">2022</dim:field>
          <dim:field mdschema="thesis" element="degree" qualifier="grantor">Harvard University Engineering and Applied Sciences</dim:field>
          <dim:field mdschema="thesis" element="degree" qualifier="level">Bachelor's</dim:field>
          <dim:field mdschema="thesis" element="degree" qualifier="name">S.B.</dim:field>
          <dim:field mdschema="thesis" element="degree" qualifier="department">Engineering Sciences SB</dim:field>
          <dim:field mdschema="dash" element="depositing" qualifier="author" authority="">Nuttall, Liam</dim:field>
          <dim:field mdschema="dash" element="author" qualifier="email" authority="">lcnuttall15@gmail.com</dim:field>
          <dim:field mdschema="dash" element="license">LAA</dim:field>
        </dim:dim>
      </xmlData>
    </mdWrap>
  </dmdSec>
  <amdSec ID="amd_whole_item">
    <rightsMD ID="rightsMD_whole_item">
      <mdWrap MDTYPE="OTHER" OTHERMDTYPE="METSRIGHTS">
        <xmlData xmlns:rights="http://cosimo.stanford.edu/sdr/metsrights/" xsi:schemaLocation="http://cosimo.stanford.edu/sdr/metsrights/ http://www.loc.gov/standards/rights/METSRights.xsd">
          <rights:RightsDeclarationMD RIGHTSCATEGORY="LICENSED">
            <rights:Context in-effect="true" CONTEXTCLASS="GENERAL PUBLIC">
              <rights:Permissions DISCOVER="true" DISPLAY="true" MODIFY="false" DELETE="false"/>
            </rights:Context>
          </rights:RightsDeclarationMD>
        </xmlData>
      </mdWrap>
    </rightsMD>
    <sourceMD ID="sourceMD_whole_item">
      <mdWrap MDTYPE="OTHER" OTHERMDTYPE="AIP-TECHMD">
        <xmlData xmlns:dim="http://www.dspace.org/xmlns/dspace/dim">
          <dim:dim dspaceType="ITEM">
            <dim:field mdschema="dc" element="creator">disspub@proquest.com</dim:field>
          </dim:dim>
        </xmlData>
      </mdWrap>
    </sourceMD>
  </amdSec>
  <amdSec ID="amd_primary">
    <rightsMD ID="rightsMD_primary">
      <mdWrap MDTYPE="OTHER" OTHERMDTYPE="METSRIGHTS">
        <xmlData xmlns:rights="http://cosimo.stanford.edu/sdr/metsrights/" xsi:schemaLocation="http://cosimo.stanford.edu/sdr/metsrights/ http://www.loc.gov/standards/rights/METSRights.xsd">
          <rights:RightsDeclarationMD RIGHTSCATEGORY="LICENSED">
            <rights:Context in-effect="true" CONTEXTCLASS="MANAGED GRP">
              <rights:UserName USERTYPE="GROUP">Staff</rights:UserName>
              <rights:Permissions DISCOVER="true" DISPLAY="true" MODIFY="false" DELETE="false"/>
            </rights:Context>
            <rights:Context in-effect="true" CONTEXTCLASS="GENERAL PUBLIC">
              <rights:Permissions DISCOVER="true" DISPLAY="true" MODIFY="false" DELETE="false"/>
            </rights:Context>
          </rights:RightsDeclarationMD>
        </xmlData>
      </mdWrap>
    </rightsMD>
    <sourceMD ID="sourceMD_primary">
      <mdWrap MDTYPE="OTHER" OTHERMDTYPE="AIP-TECHMD">
        <xmlData xmlns:dim="http://www.dspace.org/xmlns/dspace/dim">
          <dim:dim dspaceType="BITSTREAM">
            <dim:field mdschema="dc" element="title">ES 100 Final Thesis PDF - Liam Nuttall.pdf</dim:field>
            <dim:field mdschema="dc" element="format" qualifier="mimetype">application/pdf</dim:field>
            <dim:field mdschema="dc" element="format" qualifier="supportlevel">KNOWN</dim:field>
            <dim:field mdschema="dc" element="format" qualifier="internal">false</dim:field>
          </dim:dim>
        </xmlData>
      </mdWrap>
    </sourceMD>
  </amdSec>
  <amdSec ID="amd_license_2006883">
    <rightsMD ID="rightsMD_license_2006883">
      <mdWrap MDTYPE="OTHER" OTHERMDTYPE="METSRIGHTS">
        <xmlData xmlns:rights="http://cosimo.stanford.edu/sdr/metsrights/" xsi:schemaLocation="http://cosimo.stanford.edu/sdr/metsrights/ http://www.loc.gov/standards/rights/METSRights.xsd">
          <rights:RightsDeclarationMD RIGHTSCATEGORY="LICENSED"/>
        </xmlData>
      </mdWrap>
    </rightsMD>
    <sourceMD ID="sourceMD_license_2006883">
      <mdWrap MDTYPE="OTHER" OTHERMDTYPE="AIP-TECHMD">
        <xmlData xmlns:dim="http://www.dspace.org/xmlns/dspace/dim">
          <dim:dim dspaceType="BITSTREAM">
            <dim:field mdschema="dc" element="title">Harvard_IR_License_-_LAA_for_ETDs_(2020).pdf</dim:field>
            <dim:field mdschema="dc" element="format" qualifier="mimetype">application/pdf</dim:field>
          </dim:dim>
        </xmlData>
      </mdWrap>
    </sourceMD>
  </amdSec>
  <fileSec>
    <fileGrp ID="etdadmin-mets-fgrp-1" USE="CONTENT">
      <file GROUPID="etdadmin-mets-file-group" ID="etdadmin-mets-file-2090497" MIMETYPE="application/pdf" ADMID="amd_primary" SEQ="1">
        <FLocat LOCTYPE="URL" xlink:href="ES 100 Final Thesis PDF - Liam Nuttall.pdf"/>
      </file>
    </fileGrp>
    <fileGrp ID="etdadmin-mets-fgrp-2" USE="LICENSE">
      <file GROUPID="etdadmin-mets-file-group" ID="etdadmin-mets-file-2006883" MIMETYPE="application/pdf" ADMID="amd_license_2006883">
        <FLocat LOCTYPE="URL" xlink:href="Harvard_IR_License_-_LAA_for_ETDs_(2020).pdf"/>
      </file>
    </fileGrp>
  </fileSec>
  <structMap ID="etdadmin-mets-struct-1" LABEL="structure" TYPE="LOGICAL">
    <div ID="etdadmin-mets-div-1" DMDID="etdadmin-mets-dmd-1" TYPE="DSpace Item">
      <fptr FILEID="etdadmin-mets-file-2090497"/>
      <div ID="etdadmin-mets-div-2090497" TYPE="DSpace Content Bitstream">
        <fptr FILEID="etdadmin-mets-file-2090497"/>
      </div>
    </div>
  </structMap>
</mets>"""
    mets_extractor = MetsExtractor()
    mets_extractor.set_mets_data(mets_mock_file)
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

def test_mets_extractor_no_data():
    mets_extractor = MetsExtractor()
    with pytest.raises(Exception):
        mets_extractor.get_amdid_and_mimetype(
            "ES 100 Final Thesis PDF - Liam Nuttall.pdf")
    with pytest.raises(Exception):
        mets_extractor.get_degree_date()
    with pytest.raises(Exception):
        mets_extractor.get_dash_id()

def test_mets_extractor_missing_in_mets():
    mets_mock_file = """<?xml version="1.0" encoding="UTF-8" standalone="no"?>
        <mets xmlns="http://www.loc.gov/METS/"
        xmlns:xlink="http://www.w3.org/1999/xlink"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ID="etd-mets-1"
        OBJID="etd-submission-892650" TYPE="DSpace ITEM"
        PROFILE="http://www.dspace.org/schema/aip/mets_aip_1_0.xsd"
        xsi:schemaLocation="http://www.loc.gov/METS/
        http://www.loc.gov/standards/mets/mets.xsd">
        </mets>"""
    mets_extractor = MetsExtractor()
    mets_extractor.set_mets_data(mets_mock_file)
    amdid_and_mimetype = mets_extractor.get_amdid_and_mimetype(
            "ES 100 Final Thesis PDF - Liam Nuttall.pdf")
    assert amdid_and_mimetype.amdid is None
    assert amdid_and_mimetype.mimetype is None
