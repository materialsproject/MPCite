import requests, json, os, logging, pybtex, pymongo, time
from datetime import datetime, timedelta, date
from tqdm import *

logger = logging.getLogger('mpcite')

class DoiBuilder(object):
    """Builder to obtain DOIs for all/new materials"""
    def __init__(self, adapter, explorer, limit=1):
        self.ad = adapter # OstiMongoAdapter
        self.auth = None
        if explorer.user and explorer.password:
            self.auth = (explorer.user, explorer.password)
        self.endpoint = explorer.endpoint
        self.limit = limit
        self.show_pbar = False

    @property
    def limit(self):
        return self.__limit

    @property
    def show_pbar(self):
        return self.__show_pbar

    @limit.setter
    def limit(self, nr_requested_dois):
        if nr_requested_dois > 0:
            self.__limit = 2 * nr_requested_dois
        else:
            logger.info(
                'invalid # of requested DOIs ({}) -> set to 1'.format(
                    nr_requested_dois
                ))
            self.__limit = 1

    @show_pbar.setter
    def show_pbar(self, flag):
        if isinstance(flag, bool):
            self.__show_pbar = flag
        else:
            logger.info('invalid show_pbar flag ({}) -> set to False').format(flag)
            self.__show_pbar = False

    def validate_dois(self):
        """update doicoll with validated DOIs"""
        weekday = date.today().weekday()
        if weekday == 0 or weekday == 6:
            day = 'Sunday' if weekday else 'Monday'
            logger.info('no validation on {}'.format(day))
            return
        mpids = list(self.ad.doicoll.find({
            'doi': {'$exists': False},
            'created_on': {'$lte': datetime.now() - timedelta(days=2)}
        }).sort('updated_on', pymongo.ASCENDING).limit(self.limit).distinct('_id'))
        if mpids:
            for mpid in mpids:
                doi = self.ad.get_doi_from_elink(mpid)
                if doi is not None:
                    validated_on = datetime.combine(date.today(), datetime.min.time())
                    self.ad.doicoll.update(
                        {'_id': mpid},
                        {'$set': {'doi': doi, 'validated_on': validated_on}}
                    )
                    logger.info('DOI {} validated for {}'.format(doi, mpid))
                time.sleep(.5)
        else:
            logger.info('no DOIs available for validation')

    def save_bibtex_item(self, doc):
        """save bibtex string for single material"""
        osti_id = doc['doi'].split('/')[-1]
        endpoint = self.endpoint + '/{}'.format(osti_id)
        headers = {'Accept': 'application/x-bibtex'}
        try:
            r = requests.get(endpoint, auth=self.auth, headers=headers)
        except Exception as ex:
            logger.error('bibtex for {} ({}) threw exception: {}'.format(
                doc['_id'], doc['doi'], ex
            ))
            return False
        if not r.status_code == 200:
            logger.error('bibtex request for {} ({}) failed w/ code {}'.format(
                doc['_id'], doc['doi'], r.status_code
            ))
            return False
        bib_data = pybtex.database.parse_string(r.content, 'bibtex')
        if len(bib_data.entries) > 0:
            bibtexed_on = datetime.combine(date.today(), datetime.min.time())
            bibtex_string = bib_data.to_string('bibtex')
            self.ad.doicoll.update(
                {'_id': doc['_id']}, {'$set': {
                    'bibtex': bibtex_string, 'bibtexed_on': bibtexed_on
                }}
            )
            if not self.show_pbar:
                logger.info('saved bibtex for {} ({})'.format(doc['_id'], doc['doi']))
        else:
            logger.info('invalid bibtex for {} ({})'.format(doc['_id'], doc['doi']))
            return False
        return bibtex_string

    def save_bibtex(self):
        """save bibtex string in doicoll for all valid DOIs w/o bibtex yet"""
        docs = self.ad.doicoll.find(
            {'doi': {'$exists': True}, 'bibtex': {'$exists': False}}
        ).sort('validated_on', pymongo.ASCENDING).limit(self.limit)
        self.loop_bibtex(docs)

    def loop_bibtex(self, docs):
        """save bibtex for a list of doicoll documents"""
        ndocs, num_bibtex_errors = docs.count(), 0
        if self.show_pbar:
            pbar = tqdm(total=ndocs)
        for doc in docs:
            if num_bibtex_errors > 2:
                logger.error('abort bibtex generation (too many request errors)')
                return None
            if not self.save_bibtex_item(doc):
                num_bibtex_errors += 1
            if self.show_pbar:
                pbar.update()
            time.sleep(.5)
        if self.show_pbar:
            pbar.close()
            logger.info('{} bibtex strings saved'.format(ndocs))

    def sync(self):
        """sync up doi and materials collections (needed after doicoll reset)"""
        existing_mp_ids = self.ad.matcoll.find(
            {'doi': {'$exists': True}}, {'_id': 0, 'task_id': 1}
        ).distinct('task_id')
        if existing_mp_ids:
            num_bibtex_errors = 0
            docs = self.ad.doicoll.find(
                {'_id': {'$in': existing_mp_ids}}, {'doi': 1, 'bibtex': 1}
            ).limit(0 if self.show_pbar else 5)
            ndocs = docs.count()
            if self.show_pbar:
                pbar = tqdm(total=ndocs)
            for doc in docs:
                if num_bibtex_errors > 2:
                    logger.error('abort bibtex generation (too many request errors)')
                    return None
                doc['bibtex'] = self.save_bibtex_item(doc)
                if not doc['bibtex']:
                    num_bibtex_errors += 1
                    continue
                self.build_item(doc)
                if self.show_pbar:
                    pbar.update()
            if self.show_pbar:
                pbar.close()
                logger.info('{} materials synced'.format(ndocs))
        else:
            logger.info('no materials with DOIs exist')

    def build_item(self, item):
        """build doi and bibtex for a single material"""
        self.ad.matcoll.update(
            {'task_id': item['_id']}, {'$set': {
                'doi': item['doi'], 'doi_bibtex': item['bibtex']
            }}
        )
        built_on = datetime.combine(date.today(), datetime.min.time())
        self.ad.doicoll.update(
            {'_id': item['_id']}, {'$set': {'built_on': built_on}}
        )
        if not self.show_pbar:
            logger.info('built {} ({}) into matcoll'.format(item['_id'], item['doi']))

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
            items = self.ad.doicoll.find(
                {'_id': {'$in': missing_mp_ids}}, {'doi': 1, 'bibtex': 1}
            ).sort('bibtexed_on', pymongo.ASCENDING)
            if self.show_pbar:
                pbar = tqdm(total=items.count())
            for item in items:
                self.build_item(item)
                if self.show_pbar:
                    pbar.update()
            if self.show_pbar:
                pbar.close()
                logger.info('all available DOIs built into matcoll')
        else:
          logger.info('no valid DOIs available for build')
