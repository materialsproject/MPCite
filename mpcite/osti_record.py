import os, logging, sys
from collections import OrderedDict
from dicttoxml import dicttoxml
from xml.dom.minidom import parseString
from pybtex.database.input import bibtex
from StringIO import StringIO
from xmltodict import parse
from adapter import OstiMongoAdapter

logger = logging.getLogger('mpcite')

class OstiRecord(object):
    """object defining a MP-specific record for OSTI"""
    def __init__(self, l=None, n=0, doicoll=None, matcoll=None,
                 db_yaml='materials_db_dev.yaml'):
        self.endpoint = os.environ['OSTI_ENDPOINT']
        self.bibtex_parser = bibtex.Parser()
        self.ad = OstiMongoAdapter.from_config(db_yaml=db_yaml) \
            if doicoll is None or matcoll is None else \
            OstiMongoAdapter.from_collections(doicoll, matcoll)
        self.materials = self.ad.get_materials_cursor(l, n)
        research_org = 'Lawrence Berkeley National Laboratory (LBNL), Berkeley, CA (United States)'
        self.records = []
        for material in self.materials:
            self.material = material
            # prepare record
            self.records.append(OrderedDict([
                ('osti_id', self.ad.get_osti_id(material)),
                ('dataset_type', 'SM'),
                ('title', self._get_title()),
                ('creators', 'Kristin Persson'),
                ('product_nos', self.material['task_id']),
                ('accession_num', self.material['task_id']),
                ('contract_nos', 'AC02-05CH11231; EDCBEE'),
                ('originating_research_org', research_org),
                ('publication_date', self._get_publication_date()),
                ('language', 'English'),
                ('country', 'US'),
                ('sponsor_org', 'USDOE Office of Science (SC), Basic Energy Sciences (BES) (SC-22)'),
                ('site_url', self._get_site_url(self.material['task_id'])),
                ('contact_name', 'Kristin Persson'),
                ('contact_org', 'LBNL'),
                ('contact_email', 'kapersson@lbl.gov'),
                ('contact_phone', '+1(510)486-7218'),
                ('related_resource', 'https://materialsproject.org/citing'),
                ('contributor_organizations', 'MIT; UC Berkeley; Duke; U Louvain'), # not listed in research_org
                ('subject_categories_code', '36 MATERIALS SCIENCE'),
                ('keywords', self._get_keywords()),
                ('description', 'Computed materials data using density '
                 'functional theory calculations. These calculations determine '
                 'the electronic structure of bulk materials by solving '
                 'approximations to the Schrodinger equation. For more '
                 'information, see https://materialsproject.org/docs/calculations')
            ]))
            if not self.records[-1]['osti_id']:
                self.records[-1].pop('osti_id', None)
        if not self.records:
            logger.info('No materials available for DOI requests')
            sys.exit(0)
        self.records_xml = parseString(dicttoxml(
            self.records, custom_root='records', attr_type=False
        ))
        items = self.records_xml.getElementsByTagName('item')
        for item in items:
            self.records_xml.renameNode(item, '', item.parentNode.nodeName[:-1])
        logger.debug(self.records_xml.toprettyxml())

    def submit(self):
        """submit generated records to OSTI"""
        logger.info('start submission of OSTI records')
        content = self.ad.osti_request(
            req_type='post', payload=self.records_xml.toxml()
        )
        dois = {}
        for ridx,record in enumerate(content['records']):
            if record['status'] == 'SUCCESS' or \
               record['status_message'] == 'Duplicate URL Found.;':
                print record['doi']
                dois[record['product_nos']] = {
                    'doi': record['doi'],
                    'updated': bool('osti_id' in self.records[ridx])
                }
            else:
                logger.warning('ERROR for %s: %s' % (
                    record['product_nos'], record['status_message']
                ))
        self.ad.insert_dois(dois)

    def _get_title(self):
        formula = self.material['pretty_formula']
        sg_num = self.material['spacegroup']['number']
        return 'Materials Data on %s (SG:%d) by Materials Project' % (
            formula, sg_num
        )

    def _get_creators(self):
        creators = []
        for author in self.material['snl_final']['about']['authors']:
            names = author['name'].split()
            last_name = names[-1]
            first_name = ' '.join(names[:-1])
            creators.append(', '.join([last_name, first_name]))
        return '; '.join(creators)

    def _get_publication_date(self):
        return self.material['created_at'].strftime('%m/%d/%Y')

    def _get_site_url(self, mp_id):
        return 'https://materialsproject.org/materials/%s' % mp_id

    def _get_related_resource(self):
        bib_data = self.bibtex_parser.parse_stream(StringIO(
            self.material['snl_final']['about']['references']
        ))
        related_resource = []
        for entry in bib_data.entries.values():
            related_resource.append(entry.fields.get('url'))
        return ', '.join(filter(None, related_resource))

    def _get_keywords(self):
        keywords = '; '.join([
            'crystal structure',
            self.material['snl_final']['reduced_cell_formula_abc'],
            self.material['snl_final']['chemsystem'],
            '; '.join([
                '-'.join(['ICSD', str(iid)]) for iid in self.material['icsd_ids']
            ]),
        ])
        keywords += '; electronic bandstructure' if self.material['has_bandstructure'] else ''
        return keywords
