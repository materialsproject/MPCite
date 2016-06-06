import requests, json, os, datetime, logging, pybtex
from adapter import OstiMongoAdapter

logger = logging.getLogger('mpcite')

class DoiBuilder(object):
    """Builder to obtain DOIs for all/new materials"""
    def __init__(self, db_yaml='materials_db_dev.yaml'):
        self.ad = OstiMongoAdapter.from_config(db_yaml=db_yaml)

    def validate_dois(self):
        """update doicoll with validated DOIs"""
        mpids = list(self.ad.doicoll.find({'doi': {'$exists': False}}).distinct('_id'))
        if mpids:
            for mpid in mpids:
                doi = self.ad.get_doi_from_elink(mpid)
                if doi is not None:
                    self.ad.doicoll.update({'_id': mpid}, {'$set': {'doi': doi}})
                    logger.info('DOI {} validated for {}'.format(doi, mpid))
        else:
            logger.info('no DOIs available for validation')

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
            auth = (os.environ['OSTI_EXPLORER_USER'], os.environ['OSTI_EXPLORER_PASSWORD'])
            endpoint = os.environ['OSTI_EXPLORER_ENDPOINT'] + '/{}'.format(osti_id)
            headers = {'Accept': 'application/x-bibtex'}
            try:
                r = requests.get(endpoint, auth=auth, headers=headers)
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
            bib_data = pybtex.database.parse_string(r.content, 'bibtex')
            if len(bib_data.entries) > 0:
                self.ad.doicoll.update(
                    {'_id': doc['_id']},
                    {'$set': {'bibtex': bib_data.to_string('bibtex')}}
                )
                logger.info('saved bibtex for {} ({})'.format(doc['_id'], doc['doi']))
            else:
                logger.info('invalid bibtex for {} ({})'.format(doc['_id'], doc['doi']))
                num_bibtex_errors += 1

    def build(self):
        """build DOIs into matcoll"""
        # get mp-id's
        #     - w/ valid doi & bibtex keys in doicoll
        #     - but w/o doi & doi_bibtex keys in matcoll
        valid_mp_ids = self.ad.doicoll.find({
            'doi': {'$exists': True}, 'bibtex': {'$exists': True}
        }).distinct('_id')
        if valid_mp_ids:
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
        else:
          logger.info('no valid DOIs available for build')
