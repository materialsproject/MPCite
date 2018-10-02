import logging, requests, pybtex, yaml, os, time
from datetime import datetime, timedelta, date
from xmltodict import parse
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
            '$or': [{'valid': False}, {'doi': {'$exists': False}}, {'bibtex': {'$exists': False}}]
        }
        mpids = self.dois.distinct(self.dois.key, query)
        self.total = len(mpids)
        self.logger.info('{} materials to process'.format(self.total))

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

        # validate DOI
        time.sleep(.5)
        doi, status = self.get_doi_from_elink(material_id)
        self.logger.debug('{}: {} ({})'.format(material_id, doi, status))
        doi_doc.update({'doi': doi, 'status': status, 'valid': False})
        ready = bool(status == 'COMPLETED' or (status == 'PENDING' and doi))
        if ready and self.num_bibtex_errors < 3:
            try:
                doi_doc['bibtex'] = self.get_bibtex(doi)
                doi_doc['valid'] = True
            except ValueError:
                self.num_bibtex_errors += 1

        # TODO record generation and submission
        #if not rec.generate(num_or_list):
        #    return
        #rec.submit()

        self.logger.debug(doi_doc)
        return doi_doc

    def update_targets(self, items):
        self.logger.info("No items to update")

    ########### utilities ###################

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
            msg = '{} not in E-Link. Run `mpcite update`?'.format(mpid_or_ostiid)
            self.logger.error(msg)
            raise ValueError(msg)
        doi = content['records'][0]['doi']
        return doi['#text'], doi['@status']

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
            msg = 'bibtex for {} threw exception: {}'.format(doi, ex)
            self.logger.error(msg)
            raise ValueError(msg)
        if not r.status_code == 200:
            msg = 'bibtex request for {} failed w/ code {}'.format(doi, r.status_code)
            self.logger.error(msg)
            raise ValueError(msg)
        bib_data = pybtex.database.parse_string(r.content, 'bibtex')
        if len(bib_data.entries) > 0:
            return bib_data.to_string('bibtex')
        else:
            msg = 'invalid bibtex for {}'.format(doi)
            self.logger.error(msg)
            raise ValueError(msg)


class DoiCopyBuilder(CopyBuilder):

    def process_item(self, item):
        doc = {'_id': item['_id'], 'task_id': item['_id']}
        doc['valid'] = 'validated_on' in item
        for k in ['doi', 'bibtex', 'last_updated']:
          doc[k] = item.get(k)
        return doc

