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

    def build(self, mpids=None):
        """build DOIs into matcoll"""
        # get mp-id's
        #     - w/ valid doi & bibtex keys in doicoll
        #     - but w/o doi & doi_bibtex keys in matcoll
        query = {'doi': {'$exists': True}, 'bibtex': {'$exists': True}}
        if mpids is not None:
            query['_id'] = {'$in': mpids}
        valid_mp_ids = self.ad.doicoll.find(query).distinct('_id')
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
