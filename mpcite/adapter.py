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
        auth = (os.environ['OSTI_ELINK_USER'], os.environ['OSTI_ELINK_PASSWORD'])
        endpoint = os.environ['OSTI_ELINK_ENDPOINT']
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
                    doc['doi'] = record['doi']['#text']
                doi_docs.append(doc)
            docs_inserted = self.doicoll.insert(doi_docs)
            logger.info('{} DOIs inserted into doicoll'.format(len(docs_inserted)))

    def get_traces(self):
        from plotly.graph_objs import Scatter
        traces = [
            Scatter(x=[], y=[], name='total materials'),
            Scatter(x=[], y=[], name='total requested DOIs'),
            Scatter(x=[], y=[], name='total validated DOIs'),
            Scatter(x=[], y=[], name='new materials'),
            Scatter(x=[], y=[], name='newly requested DOIs'),
            Scatter(x=[], y=[], name='newly validated DOIs'),
        ]
        num_requested_dois = 0
        for doc in self.doicoll.aggregate([
            {'$group': {'_id': '$created_on', 'num': {'$sum': 1}}},
            {'$sort': {'_id': 1}},
        ]):
            num_requested_dois += doc['num']
            for trace in traces:
                trace.x.append(doc['_id'].date())
            traces[4].y.append(doc['num'])
            traces[1].y.append(num_requested_dois)
        num_validated_dois = 0
        for doc in self.doicoll.aggregate([
            {'$match': {'doi': {'$exists': True}}},
            {'$group': {'_id': '$created_on', 'num': {'$sum': 1}}},
            {'$sort': {'_id': 1}},
        ]):
            num_validated_dois += doc['num']
            for trace in traces:
                trace.x.append(doc['_id'].date())
            traces[5].y.append(doc['num'])
            traces[2].y.append(num_validated_dois)
        return traces

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
        if l is None:
            existent_mpids = self.doicoll.find().distinct('_id')
            return self.matcoll.find({
                'doi': {'$exists': False}, 'task_id': {'$nin': existent_mpids}
            }, limit=n)
        else:
            mp_ids = [ 'mp-{}'.format(el) for el in l ]
            return self.matcoll.find({'task_id': {'$in': mp_ids}})

    def get_doi_from_elink(self, mpid):
        content = self.osti_request(payload={'site_unique_id': mpid})
        doi = content['records'][0]['doi']
        if doi is None:
            logger.info('DOI for {} not valid yet'.format(mpid))
        return doi

    def get_duplicate(self, mpid):
        dup = self.duplicates.get(mpid)
        if dup is None:
            logger.error('missing DOI for duplicate {}! DB reset?'.format(mpid))
            sys.exit(1)
        dup['created_on'] = datetime.combine(dup['created_on'], datetime.min.time())
        logger.info('found DOI {} for {} in dup-file'.format(dup['doi'], mpid))
        return dup

    def get_osti_id(self, mpid):
        # empty osti_id = new submission -> new DOI
        # check for existing doi to distinguish from edit/update scenario
        doi_entry = self.doicoll.find_one({'_id': mpid})
        if doi_entry is not None and 'doi' not in doi_entry:
            logger.error('not updating {} due to pending DOI'.format(mpid))
            return None
        return '' if doi_entry is None else doi_entry['doi'].split('/')[-1]

    def insert_dois(self, dois):
        """save doi info to doicoll, only record update time if exists"""
        dois_insert, dois_update = [], []
        for mpid, doc in dois.iteritems():
            if doc.pop('updated'):
                dois_update.append(mpid)
            else:
                doc['_id'] = mpid
                if 'created_on' not in doc:
                    doc['created_on'] = datetime.now().isoformat()
                dois_insert.append(doc)
        if dois_insert:
            docs_inserted = self.doicoll.insert(dois_insert)
            logger.info('{} DOIs inserted'.format(len(docs_inserted)))
        if dois_update:
            ndocs_updated = self.doicoll.update(
                {'_id': {'$in': dois_update}},
                {'$set': {'updated_on': datetime.now().isoformat()}},
                multi=True
            )['nModified']
            logger.info('{} DOI docs updated'.format(ndocs_updated))
