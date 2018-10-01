import logging, requests, pybtex, yaml, os
from datetime import datetime, timedelta, date
from time import time
from maggma.builder import Builder
from emmet.common.copybuilder import CopyBuilder

mod_dir = os.path.dirname(os.path.abspath(__file__))
default_config = os.path.normpath(os.path.join(mod_dir, os.pardir, 'files', 'config.yaml'))

class DictAsMember(dict):
    # http://stackoverflow.com/questions/10761779/when-to-use-getattr/10761899#10761899
    def __getattr__(self, name):
        value = self[name]
        if isinstance(value, dict):
            value = DictAsMember(value)
        return value

class DoiBuilder(Builder):

    def __init__(self, materials, dois, **kwargs):
        """
        obtain DOIs for all/new materials
        Args:
            materials (Store): Store of materials documents
            dois (Store): Store of DOIs data
        """
        self.materials = materials
        self.dois = dois
        self.num_bibtex_errors = 0

        with open(default_config, 'r') as f:
            config = DictAsMember(yaml.load(f))

        self.elink = config.osti.elink
        self.explorer = config.osti.explorer

        super().__init__(sources=[materials], targets=[dois], **kwargs)

    def get_items(self):
        """
        Gets all materials that need a DOI
        Returns:
            generator of materials to retrieve/build DOI
        """
        self.logger.info("DoiBuilder Started")
        #self.logger.info("Setting indexes")
        #self.ensure_indicies()
        #q = self.materials.lu_filter(self.dois)
        #updated_mats = set(self.materials.distinct(self.materials.key, q))

        #weekday = date.today().weekday()
        #if weekday == 0 or weekday == 6:
        #    day = 'Sunday' if weekday else 'Monday'
        #    self.logger.info('no validation on {}'.format(day))
        #    return []

        query = {
            'doi': {'$exists': False},
            'created_on': {'$lte': datetime.now() - timedelta(days=2)}
        }
        mpids = self.dois.distinct(self.dois.key, query)
        self.total = len(mpids)
        self.logger.info(self.total)

        return self.materials.query(
            criteria={self.materials.key: {'$in': mpids}},
            properties=[self.materials.key]#, "structure", self.materials.lu_field],
        )


    def process_item(self, item):
        """
        build current document with DOI info
        Args:
            item (dict): a dict with a material_id and ...?
        Returns:
            dict: a DOI dict
        """
        material_id = item[self.materials.key]
        self.logger.debug("get DOI doc for {}".format(material_id))
        doi_doc = {self.materials.key: material_id}
        doi_doc.update(self.validate_doi(material_id))
        if 'doi' in doi_doc and self.num_bibtex_errors < 3:
            doi_doc.update(self.get_bibtex(doi_doc['doi']))
            if 'bibtex' not in doi_doc:
                self.num_bibtex_errors += 1

        # TODO record generation and submission
        #if not rec.generate(num_or_list):
        #    return
        #rec.submit()

        return doi_doc

    def update_targets(self, items):
        self.logger.info("No items to update")

    def osti_request(self, req_type='get', payload=None):
        self.logger.debug('{} request to {} w/ payload {} ...'.format(
            req_type, self.elink.endpoint, payload
        ))
        auth = (self.elink.user, self.elink.password)
        if req_type == 'get':
            r = requests.get(self.elink.endpoint, auth=auth, params=payload)
        elif req_type == 'post':
            r = requests.post(self.elink.endpoint, auth=auth, data=payload)
        else:
            self.logger.error('unsupported request type {}'.format(req_type))
            sys.exit(1)
        if r.status_code != 200:
            self.logger.error('request failed w/ code {}'.format(r.status_code))
            sys.exit(1)
        content = parse(r.content)['records']
        if 'record' in content:
            records = content.pop('record')
        else:
            self.logger.error('no record found for payload {}'.format(payload))
            return None
        content['records'] = records if isinstance(records, list) else [records]
        return content

    def get_doi_from_elink(self, mpid_or_ostiid):
        key = 'site_unique_id' if 'mp-' in mpid_or_ostiid \
                or 'mvc-' in mpid_or_ostiid else 'osti_id'
        content = self.osti_request(payload={key: mpid_or_ostiid})
        if content is None:
            self.logger.error('{} not in E-Link. Run `mpcite update`?'.format(mpid_or_ostiid))
            return None, None
        doi = content['records'][0]['doi']
        valid = (
          doi['@status'] == 'COMPLETED' or (
            doi['@status'] == 'PENDING' and doi['#text']
          )
        )
        if not valid:
            self.logger.error('DOI for {} not valid yet'.format(mpid_or_ostiid))
            return None, None
        return doi['#text'], doi['@status']


    def validate_doi(self, material_id):
        """validate DOI for a single material"""
        time.sleep(.5)
        doi, status = self.get_doi_from_elink(material_id)
        if doi is not None:
            validated_on = datetime.combine(date.today(), datetime.min.time())
            self.logger.info('DOI {} validated for {}'.format(doi, material_id))
            return {'doi': doi, 'validated_on': validated_on}
        return {}

    def get_bibtex(self, doi):
        """get bibtex string for single material/doi"""
        time.sleep(.5)
        osti_id = doi.split('/')[-1]
        endpoint = self.explorer.endpoint + '/{}'.format(osti_id)
        auth = None
        if self.explorer.user and self.explorer.password:
            auth = (self.explorer.user, self.explorer.password)
        headers = {'Accept': 'application/x-bibtex'}
        try:
            r = requests.get(endpoint, auth=auth, headers=headers)
        except Exception as ex:
            self.logger.error('bibtex for {} threw exception: {}'.format(doi, ex))
            return {}
        if not r.status_code == 200:
            self.logger.error('bibtex request for {} failed w/ code {}'.format(doi, r.status_code))
            return {}
        bib_data = pybtex.database.parse_string(r.content, 'bibtex')
        if len(bib_data.entries) > 0:
            bibtexed_on = datetime.combine(date.today(), datetime.min.time())
            bibtex_string = bib_data.to_string('bibtex')
            return {'bibtex': bibtex_string, 'bibtexed_on': bibtexed_on}
        else:
            self.logger.info('invalid bibtex for {}'.format(doi))
            return {}



class DoiCopyBuilder(CopyBuilder):

    def __init__(self, source, target, **kwargs):
        super().__init__(source=source, target=target, key='_id',
                         incremental=False, query=None, **kwargs)

    def process_item(self, item):
        doc = {'task_id': item['_id']}
        doc['valid'] = 'validated_on' in item
        for k in ['doi', 'bibtex']:
          doc[k] = item.get(k)
        return doc

