import os, logging, sys
from collections import OrderedDict
from dicttoxml import dicttoxml
from xml.dom.minidom import parseString
from pybtex.database.input import bibtex
from StringIO import StringIO
from xmltodict import parse
from tqdm import *

logger = logging.getLogger('mpcite')

class OstiRecord(object):
    """object defining a MP-specific record for OSTI"""
    def __init__(self, adapter):
        self.bibtex_parser = bibtex.Parser()
        self.ad = adapter # OstiMongoAdapter
        self.show_pbar = False

    @property
    def show_pbar(self):
        return self.__show_pbar

    @show_pbar.setter
    def show_pbar(self, flag):
        if isinstance(flag, bool):
            self.__show_pbar = flag
        else:
            logger.info('invalid show_pbar flag ({}) -> set to False').format(flag)
            self.__show_pbar = False

    def generate(self, num_or_list):
        # generate records for a number of not-yet-submitted materials
        # OR generate records for list of specific materials (submitted or not)
        research_org = 'Lawrence Berkeley National Laboratory (LBNL), Berkeley, CA (United States)'
        self.records = []
        cursor = self.ad.get_materials_cursor(num_or_list)
        if self.show_pbar:
            pbar = tqdm(total=cursor.count())
        for material in cursor:
            mpid = material['task_id']
            osti_id = self.ad.get_osti_id(mpid)
            if osti_id is None: continue
            # prepare record
            self.records.append(OrderedDict([
                ('osti_id', osti_id),
                ('dataset_type', 'SM'),
                ('title', self._get_title(material)),
                ('creators', 'Kristin Persson'),
                ('product_nos', mpid),
                ('accession_num', mpid),
                ('contract_nos', 'AC02-05CH11231; EDCBEE'),
                ('originating_research_org', research_org),
                ('publication_date', material['created_at'].strftime('%m/%d/%Y')),
                ('language', 'English'),
                ('country', 'US'),
                ('sponsor_org', 'USDOE Office of Science (SC), Basic Energy Sciences (BES) (SC-22)'),
                ('site_url', self._get_site_url(mpid)),
                ('contact_name', 'Kristin Persson'),
                ('contact_org', 'LBNL'),
                ('contact_email', 'kapersson@lbl.gov'),
                ('contact_phone', '+1(510)486-7218'),
                ('related_resource', 'https://materialsproject.org/citing'),
                ('contributor_organizations', 'MIT; UC Berkeley; Duke; U Louvain'), # not listed in research_org
                ('subject_categories_code', '36 MATERIALS SCIENCE'),
                ('keywords', self._get_keywords(material)),
                ('description', 'Computed materials data using density '
                 'functional theory calculations. These calculations determine '
                 'the electronic structure of bulk materials by solving '
                 'approximations to the Schrodinger equation. For more '
                 'information, see https://materialsproject.org/docs/calculations')
            ]))
            if not self.records[-1]['osti_id']:
                self.records[-1].pop('osti_id', None)
            if self.show_pbar:
                pbar.update()
        if self.show_pbar:
            pbar.close()
        if not self.records:
            logger.info('No materials available for DOI requests')
            sys.exit(0)
        logger.info('generated {} XML records'.format(len(self.records)))
        self.records_xml = parseString(dicttoxml(
            self.records, custom_root='records', attr_type=False
        ))
        items = self.records_xml.getElementsByTagName('item')
        for item in items:
            self.records_xml.renameNode(item, '', item.parentNode.nodeName[:-1])
        logger.debug(self.records_xml.toprettyxml())
        logger.info('prepared XML string for submission to OSTI')

    def submit(self):
        """submit generated records to OSTI"""
        if not self.show_pbar:
            logger.info('start submission of OSTI records')
        content = self.ad.osti_request(
            req_type='post', payload=self.records_xml.toxml()
        )
        dois = {}
        for ridx,record in enumerate(content['records']):
            mpid = record['product_nos']
            updated = bool('osti_id' in self.records[ridx])
            if record['status'] == 'SUCCESS':
                if not self.show_pbar:
                    logger.info('{} -> {}'.format(mpid, record['status']))
                dois[mpid] = {'updated': updated}
            elif record['status_message'] == 'Duplicate URL Found.;':
                # add DOI from duplicates backup since it should already be in doicoll
                # a adapter._reset is probably necessary if that's not the case
                logger.error('{} -> {}'.format(mpid, record['status_message']))
                dois[mpid] = self.ad.get_duplicate(mpid)
                dois[mpid]['updated'] = False
            else:
                logger.error('ERROR for %s: %s' % (mpid, record['status_message']))
        if dois:
            self.ad.insert_dois(dois)

    def _get_title(self, material):
        formula = material['pretty_formula']
        sg_num = material['spacegroup']['number']
        return 'Materials Data on %s (SG:%d) by Materials Project' % (
            formula, sg_num
        )

    def _get_creators(self, material):
        creators = []
        for author in material['snl_final']['about']['authors']:
            names = author['name'].split()
            last_name = names[-1]
            first_name = ' '.join(names[:-1])
            creators.append(', '.join([last_name, first_name]))
        return '; '.join(creators)

    def _get_site_url(self, mp_id):
        return 'https://materialsproject.org/materials/%s' % mp_id

    def _get_related_resource(self, material):
        bib_data = self.bibtex_parser.parse_stream(StringIO(
            material['snl_final']['about']['references']
        ))
        related_resource = []
        for entry in bib_data.entries.values():
            related_resource.append(entry.fields.get('url'))
        return ', '.join(filter(None, related_resource))

    def _get_keywords(self, material):
        keywords = '; '.join([
            'crystal structure',
            material['snl_final']['reduced_cell_formula_abc'],
            material['snl_final']['chemsystem'],
            '; '.join([
                '-'.join(['ICSD', str(iid)]) for iid in material['icsd_ids']
            ]),
        ])
        keywords += '; electronic bandstructure' if material['has_bandstructure'] else ''
        return keywords
