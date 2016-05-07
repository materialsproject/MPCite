import os, requests, logging, sys
from datetime import datetime
from pymongo import MongoClient
from monty.serialization import loadfn
from xmltodict import parse

logger = logging.getLogger('mpcite')

class OstiMongoAdapter(object):
    """adapter to connect to materials database and collection"""
    def __init__(self, doicoll, matcoll, db_yaml):
        self.matcoll = matcoll
        self.doicoll = doicoll
        dup_file_suffix = 'prod' if 'dev' not in db_yaml else 'dev'
        duplicates_file = os.path.join(
            os.getcwd(), "files", "duplicates_{}.yaml".format(dup_file_suffix)
        )
        self.duplicates = loadfn(duplicates_file) \
                if os.path.exists(duplicates_file) else {}

    @classmethod
    def from_config(cls, db_yaml='materials_db_dev.yaml'):
        config = loadfn(os.path.join(os.environ['DB_LOC'], db_yaml))
        client = MongoClient(config['host'], config['port'], j=False)
        db = client[config['db']]
        db.authenticate(config['username'], config['password'])
        return OstiMongoAdapter(db.dois, db.materials, db_yaml)

    def osti_request(self, req_type='get', payload=None):
        logger.debug('{} request w/ payload {} ...'.format(req_type, payload))
        auth = (os.environ['OSTI_USER'], os.environ['OSTI_PASSWORD'])
        endpoint = os.environ['OSTI_ENDPOINT']
        if req_type == 'get':
            r = requests.get(endpoint, auth=auth, params=payload)
        elif req_type == 'post':
            r = requests.post(endpoint, auth=auth, data=payload)
        else:
            logger.error('unsupported request type {}'.format(req_type))
            sys.exit(1)
        if r.status_code != 200:
            logger.error('request failed w/ code {}'.format(r.status_code))
            sys.exit(1)
        content = parse(r.content)['records']
        records = content.pop('record')
        content['records'] = records if isinstance(records, list) else [records]
        return content

    def _reset(self):
        """remove `doi` keys from matcoll, clear and reinit doicoll"""
        matcoll_clean = self.matcoll.update(
            {'doi': {'$exists': True}}, {'$unset': {'doi': 1, 'doi_bibtex': 1}},
            multi=True
        )
        if matcoll_clean['ok']:
            logger.info('DOI info cleaned from matcoll')
        else:
            logger.error('DOI cleaning of matcoll failed!')
            return
        doicoll_remove = self.doicoll.remove()
        if doicoll_remove['ok']:
            logger.info('doi collection removed.')
        else:
            logger.error('DOI collection removal failed!')
            return
        start_record, remaining_num_records = 0, sys.maxsize
        while remaining_num_records > 0:
            content = self.osti_request(payload={'start': start_record})
            page_size = int(content['@rows'])
            if start_record == 0:
                remaining_num_records = int(content['@numfound']) - page_size
            else:
                remaining_num_records -= page_size
            start_record += page_size
            doi_docs = []
            for ridx,record in enumerate(content['records']):
                created_on = datetime.strptime(record['date_first_submitted'], "%Y-%m-%d")
                updated_on = datetime.strptime(record['date_last_submitted'], "%Y-%m-%d")
                doc = {
                    '_id': record['product_nos'],
                    'created_on': created_on, 'updated_on': updated_on
                }
                if record['doi'] is not None:
                    doc['doi'] = record['doi']
                doi_docs.append(doc)
            docs_inserted = self.doicoll.insert(doi_docs)
            logger.info('{} DOIs inserted into doicoll'.format(len(docs_inserted)))

    def get_all_dois(self):
        # NOTE: doi info saved in matcoll as `doi` and `doi_bibtex`
        dois = {}
        for doc in self.matcoll.find(
            {'doi': {'$exists': True}},
            {'_id': 0, 'task_id': 1, 'doi': 1}
        ):
            dois[doc['task_id']] = doc['doi']
        return dois

    def get_materials_cursor(self, l, n):
        mpids_exclude = [
            'mp-12661', 'mp-4', 'mp-12661', 'mp-20379', 'mp-188', 'mp-4283',
            'mp-12662', 'mp-30', 'mp-549970', 'mp-12660', 'mp-22452',
            'mp-19918', 'mp-22441', 'mp-568345', 'mp-19091', 'mp-569335',
            'mp-31899', 'mp-12657', 'mp-609151', 'mp-601830', 'mp-694955'
        ]
        if l is None:
            return self.matcoll.find({
                'doi': {'$exists': False},
                #'task_id': {'$nin': self.doicoll.find().distinct('_id')}
                'task_id': {'$nin': mpids_exclude}
            }, limit=n)
        else:
            mp_ids = [ 'mp-{}'.format(el) for el in l ]
            return self.matcoll.find({'task_id': {'$in': mp_ids}})

    def get_osti_id(self, mat):
        # empty osti_id = new submission -> new DOI
        # check for existing doi to distinguish from edit/update scenario
        doi_entry = self.doicoll.find_one({'_id': mat['task_id']})
        return '' if doi_entry is None else doi_entry['doi'].split('/')[-1]

    def insert_dois(self, dois):
        """save doi info to doicoll, only record update time if exists"""
        dois_insert = [
            {'_id': mpid, 'doi': d['doi'], 'valid': False,
             'created_at': datetime.now().isoformat()}
            for mpid,d in dois.iteritems() if not d['updated']
        ]
        if dois_insert: logger.info(self.doicoll.insert(dois_insert))
        dois_update = [ mpid for mpid,d in dois.iteritems() if d['updated'] ]
        if dois_update:
            logger.info(self.doicoll.update(
                {'_id': {'$in': dois_update}},
                {'$set': {'updated_at': datetime.now().isoformat()}},
                multi=True
            ))
