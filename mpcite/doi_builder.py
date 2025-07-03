'''
doi_builder.py
A doi collection must store the following information about a document:
- doi number
- title
- osti id (ELink's Unique Identifier)
- material id (MP's Unique Identifier)
- date of system entry date (Date (UTC) of this revision's inception)
- date of last update (date edited or date_submitted_to_osti_last) (take from ELink)
- workflow status and the date (?) of each step:
    - SA, saved, in a holding state, not to be processed
    - SR, submit to releasing official "released_to_osti_date, as entered by releasing official"
    - SO, submit to OSTI 
    - SF, submitted but failed validation
    - SX, submitted but failed to release
    - SV, submitted and validated
    - R, released
- 

Here is an example of RecordResponse
RecordResponse(
    osti_id=2523296,
    workflow_status='SA',
    access_limitations=['UNL'],
    access_limitation_other=None,
    announcement_codes=None,
    availability=None,
    edition=None,
    volume=None,

    # Identifiers
    identifiers=[
        Identifier(type='CN_NONDOE', value='EDCBEE'),
        Identifier(type='CN_DOE', value='AC02-05CH11231'),
        Identifier(type='RN', value='mp-1037659'),
    ],

    # People involved
    persons=[
        Person(
            type='CONTACT',
            first_name='Kristin',
            last_name='Persson',
            phone='+1(510)486-7218',
            email=['feedback@materialsproject.org'],
            affiliations=[
                Affiliation(name='LBNL')
            ]
        )
    ],

    # Organizations
    organizations=[
        Organization(name='The Materials Project', type='CONTRIBUTING', contributor_type='ResearchGroup'),
        Organization(name='LBNL Materials Project', type='RESEARCHING'),
        Organization(name='Lawrence Berkeley National Laboratory (LBNL), Berkeley, CA (United States)', type='RESEARCHING'),
        Organization(name='USDOE Office of Science (SC), Basic Energy Sciences (BES) (SC-22)', type='SPONSOR'),
        Organization(name='MIT', type='CONTRIBUTING', contributor_type='Other'),
        Organization(name='UC Berkeley', type='CONTRIBUTING', contributor_type='Other'),
        Organization(name='Duke', type='CONTRIBUTING', contributor_type='Other'),
        Organization(name='U Louvain', type='CONTRIBUTING', contributor_type='Other'),
    ],

    # Metadata
    country_publication_code='US',
    doe_supported_flag=False,
    doi='10.17188/1714845',
    edit_reason='Record updated upon request of LBNL-MP to remove authors and replace with a single collaborator.',
    format_information='',
    invention_disclosure_flag=None,
    paper_flag=False,
    peer_reviewed_flag=False,
    product_type='DA',
    publication_date=datetime.date(2020, 4, 30),
    publication_date_text='04/30/2020',
    site_url='https://materialsproject.org/materials/mp-1037659',
    site_ownership_code='LBNL-MP',
    site_unique_id='mp-1037659',
    subject_category_code=['36'],
    title='Materials Data on RbYMg30O32 by Materials Project',

    # Description
    description="""
        RbMg₃₀YO₃₂ is Molybdenum Carbide MAX Phase-derived and crystallizes in the tetragonal P4/mmm space group.
        Rb¹⁺ is bonded to six O²⁻ atoms to form RbO₆ octahedra...
        (Truncated here for brevity, full description is included in original)
    """,

    keywords=['crystal structure', 'RbYMg30O32', 'Mg-O-Rb-Y'],
    languages=['English'],
    related_doc_info='https://materialsproject.org/citing',

    # Media
    media=[
        MediaInfo(
            media_id=1908478,
            osti_id=2523296,
            status='C',
            mime_type='text/html',
            files=[
                MediaFile(
                    media_file_id=12017281,
                    media_type='O',
                    url='https://materialsproject.org/materials/mp-1037659'
                ),
                MediaFile(
                    media_file_id=12017284,
                    media_type='C',
                    mime_type='text/html',
                    media_source='OFF_SITE_DOWNLOAD'
                )
            ]
        )
    ],

    # Audit logs
    audit_logs=[
        AuditLog(
            messages=['Revision status is not correct, found SA'],
            status='FAIL',
            type='RELEASER',
            audit_date=datetime.datetime(2025, 6, 30, 22, 30, 24, 865000, tzinfo=TzInfo(UTC))
        )
    ],

    # Timestamps
    date_metadata_added=datetime.datetime(2025, 6, 30, 22, 30, 20, 495000, tzinfo=TzInfo(UTC)),
    date_metadata_updated=datetime.datetime(2025, 6, 30, 22, 30, 22, 247000, tzinfo=TzInfo(UTC)),

    # Misc
    revision=2,
    added_by=139001,
    edited_by=139001,
    collection_type='DOE_LAB',
    hidden_flag=False
)
'''

from pydantic import BaseModel, ConfigDict
import datetime

class doi_model(BaseModel):
    # identifiers
    doi: str # can be taken from ELink API
    title: str # can be taken from ELink API
    osti_id: str # can be taken from ELink API
    material_id: str # can be taken from Robocrys Collection or ELink API

    # time stamps
    date_record_entered_onto_ELink: datetime.datetime # can be taken from ELink API response 
    date_record_last_updated_on_Elink: datetime.datetime

    # status
    elink_workflow_status: str # can be taken from ELink API
    date_released: datetime.datetime
    date_submitted_to_osti_first: datetime.datetime
    date_submitted_to_osti_last: datetime.datetime 
    date_published: datetime.datetime # labelled as publication_date in RecordResponse of ELink API

# hypothetically post an update or submit a new record and receive the RecordResponse
def RecordResponse_to_doi_model(recordresponse):
    '''
    turns a recordresponse, which is returned from a save, submission, post, etc. into a doi_model object
    '''
    params = {
        "doi": recordresponse.doi,
        "title": recordresponse.title,
        "osti_id": str(recordresponse.osti_id),
        "material_id": recordresponse.site_unique_id,

        "date_record_entered_onto_ELink": recordresponse.date_metadata_added,
        "date_record_last_updated_on_Elink": recordresponse.date_metadata_updated,

        "elink_workflow_status": recordresponse.workflow_status,
        "date_released": recordresponse.date_released,
        # date_released_to_osti = recordresponse.released_to_osti_date, # what is the difference between these??? "Date record information was released to OSTI, as entered by releasing official." always seems to be none
        "date_submitted_to_osti_first": recordresponse.date_submitted_to_osti_first, # date record was first submitted to OSTI for publication, maintained internally by E-Link
        "date_submitted_to_osti_last": recordresponse.date_submitted_to_osti_last, # most recent date record information was submitted to OSTI. Maintained internally by E-Link.
        "date_published": recordresponse.publication_date
    }

    return doi_model(**params)

def upload_doi_document_model_to_collection(doi_model, client, collection):
    x = collection.insert_one(doi_model).inserted_id
    return x