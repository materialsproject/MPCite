import requests, json, os, datetime, logging
from bs4 import BeautifulSoup
from adapter import OstiMongoAdapter

logger = logging.getLogger('mpcite')

class DoiBuilder(object):
    """Builder to obtain DOIs for all/new materials"""
    def __init__(self, db_yaml='materials_db_dev.yaml'):
        self.ad = OstiMongoAdapter.from_config(db_yaml=db_yaml)

    def validate_dois(self):
        """update doicoll with validated DOIs"""
        for mpid in self.ad.doicoll.find({'doi': {'$exists': False}}).distinct('_id'):
            content = osti_request(payload={'site_unique_id': mpid})
            doi = content['records'][0]['doi']
            if doi is not None:
                self.ad.doicoll.update({'_id': mpid}, {'$set': {'doi': doi}})
                logger.info('DOI {} validated for {}'.format(doi, mpid))
            else:
                logger.info('DOI for {} not valid yet'.format(mpid))

    def save_bibtex(self):
        """save bibtex string in doicoll for all valid DOIs w/o bibtex yet"""
        num_bibtex_errors = 0
        for doc in self.ad.doicoll.find(
            {'doi': {'$exists': True}, 'bibtex': {'$exists': False}},
            {'updated_on': 0, 'created_on': 0}
        ):
            if num_bibtex_errors > 2:
                logger.error('abort bibtex generation (too many request errors)')
                return None
            osti_id = doc['doi'].split('/')[-1]
            doi_url = 'http://www.osti.gov/dataexplorer/biblio/{}/cite/bibtex'.format(osti_id)
            try:
                r = requests.get(doi_url)
            except Exception as ex:
                logger.error('bibtex for {} ({}) threw exception: {}'.format(
                    doc['_id'], doc['doi'], ex
                ))
                num_bibtex_errors += 1
                continue
            if not r.status_code == 200:
                logger.error('bibtex request for {} ({}) failed w/ code {}'.format(
                    doc['_id'], doc['doi'], r.status_code
                ))
                num_bibtex_errors += 1
                continue
            soup = BeautifulSoup(r.content, "html.parser")
            rows = soup.find_all('div', attrs={"class" : "csl-entry"})
            if len(rows) == 1:
                bibtex = rows[0].text.strip()
                self.ad.doicoll.update(
                    {'_id': doc['_id']}, {'$set': {'bibtex': bibtex}}
                )
                logger.info('saved bibtex for {} ({})'.format(doc['_id'], doc['doi']))
            else:
                logger.info('invalid response for bibtex request for {} ({})'.format(doc['_id'], doc['doi']))

    def build(self):
        """build DOIs into matcoll"""
        # get mp-id's
        #     - w/ valid doi & bibtex keys in doicoll
        #     - but w/o doi & doi_bibtex keys in matcoll
        valid_mp_ids = self.ad.doicoll.find({
            'doi': {'$exists': True}, 'bibtex': {'$exists': True}
        }).distinct('_id')
        missing_mp_ids = self.ad.matcoll.find(
            {
                'task_id': {'$in': valid_mp_ids},
                'doi': {'$exists': False}, 'doi_bibtex': {'$exists': False}
            },
            {'_id': 0, 'task_id': 1}
        ).distinct('task_id')
        for item in self.ad.doicoll.find(
            {'_id': {'$in': missing_mp_ids}}, {'doi': 1, 'bibtex': 1}
        ):
            self.ad.matcoll.update(
                {'task_id': item['_id']}, {'$set': {
                    'doi': item['doi'], 'doi_bibtex': item['bibtex']
                }}
            )
            logger.info('built {} ({}) into matcoll'.format(item['_id'], item['doi']))
