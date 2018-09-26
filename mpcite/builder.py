import logging, requests, pybtex
from datetime import datetime, timedelta, date
from time import time
from maggma.builder import Builder

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
        self.endpoint = 'TODO'
        self.auth = 'TODO'
        super().__init__(sources=[materials], targets=[dois], **kwargs)

    def get_items(self):
        """
        Gets all materials that need a DOI
        Returns:
            generator of materials to retrieve/build DOI
        """
        self.logger.info("DoiBuilder Started")
        self.logger.info("Setting indexes")
        #self.ensure_indicies()
        #q = self.materials.lu_filter(self.dois)
        #updated_mats = set(self.materials.distinct(self.materials.key, q))

        weekday = date.today().weekday()
        if weekday == 0 or weekday == 6:
            day = 'Sunday' if weekday else 'Monday'
            self.logger.info('no validation on {}'.format(day))
            return

        query = {
            'doi': {'$exists': False},
            'created_on': {'$lte': datetime.now() - timedelta(days=2)}
        }
        mpids = self.dois.distinct(self.dois.key, query)
        self.total = len(mpids)

        return self.materials.query(
            criteria={self.materials.key: {'$in': mpids}}
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

        return doi_doc

    def validate_doi(self, material_id):
        """validate DOI for a single material"""
        time.sleep(.5)
        doi, status = self.ad.get_doi_from_elink(material_id)
        if doi is not None:
            validated_on = datetime.combine(date.today(), datetime.min.time())
            self.logger.info('DOI {} validated for {}'.format(doi, material_id))
            return {'doi': doi, 'validated_on': validated_on}
        return {}

    def get_bibtex(self, doi):
        """get bibtex string for single material/doi"""
        time.sleep(.5)
        osti_id = doi.split('/')[-1]
        endpoint = self.endpoint + '/{}'.format(osti_id)
        headers = {'Accept': 'application/x-bibtex'}
        try:
            r = requests.get(endpoint, auth=self.auth, headers=headers)
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



