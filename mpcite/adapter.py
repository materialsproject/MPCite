import os, requests, logging, sys
from datetime import date, datetime, timedelta
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from monty.serialization import loadfn
from maggma.core import Store
from xmltodict import parse
from tqdm import *
from typing import Union
from maggma.stores import MongoStore

logger = logging.getLogger('mpcite')

class OstiMongoAdapter(object):
    """adapter to connect to materials database and collection"""
    def __init__(self, materials_store: Store, dois_store: Store, robocrys_store: Store, duplicates):
        """

        :param materials_store: represent a connection to the materials store
        :param dois_store: represent a connection to the dois store
        :param robocrys_store: represent a connection to the robocrys store
        :param duplicates: idk yet
        """
        self.materials_store: Store = materials_store
        self.doi_store: Store = dois_store
        self.robocrys_store: Store = robocrys_store
        self.duplicates = duplicates

    @classmethod
    def from_config(cls, config):
        """
        generate an OstiMongoAdapter instance
        Please note that the stores(ex:materials_store) in there should NOT be connected yet.
        They should be connected in the builder interface

        :param config: config dictionary that contains materials database connection / debug database information
        :return:
            OstiMongoAdapater instance
        """
        materials_store = cls._create_mongostore(config, config_collection_name="materials_collection")
        dois_store = cls._create_mongostore(config, config_collection_name="dois_collection")
        robocrys_store = cls._create_mongostore(config, config_collection_name="robocrys_collection")

        logger.debug(f'using DB from {materials_store.name, dois_store.name, robocrys_store.name}')

        duplicates_file = os.path.expandvars(config.duplicates_file)
        duplicates = loadfn(duplicates_file) if os.path.exists(duplicates_file) else {}

        return OstiMongoAdapter(materials_store=materials_store,
                                dois_store=dois_store,
                                robocrys_store=robocrys_store,
                                duplicates=duplicates)

    @classmethod
    def _create_mongostore(cls, config, config_collection_name: str) -> MongoStore:
        """
        Helper method to create a mongoStore instance
        :param config: configuration dictionary
        :param config_collection_name: collection name to build the mongo store
        :return:
            MongoStore instance based on the configuration parameters
        """
        return MongoStore(database=config[config_collection_name]['db'],
                          collection_name=config[config_collection_name]['collection_name'],
                          host=config[config_collection_name]['host'],
                          port=config[config_collection_name]["port"],
                          username=config[config_collection_name]["username"],
                          password=config[config_collection_name]["password"],
                          key=config[config_collection_name]["key"] if "key" in config[config_collection_name] else "task_id")

    def _reset(self, matcoll=False, rows=None):
        """remove `doi` keys from matcoll, clear and reinit doicoll"""
        if matcoll:
            matcoll_clean = self.materials_store.update(
                {'doi': {'$exists': True}}, {'$unset': {'doi': 1, 'doi_bibtex': 1}},
                multi=True
            )
            if matcoll_clean['ok']:
                logger.info('DOI info cleaned from matcoll')
            else:
                logger.error('DOI cleaning of matcoll failed!')
                return
        doicoll_remove = self.doi_store.remove()
        if doicoll_remove['ok']:
            logger.info('doi collection removed.')
        else:
            logger.error('DOI collection removal failed!')
            return
        start_record, remaining_num_records = 0, sys.maxsize
        while remaining_num_records > 0:
            payload = {'start': start_record}
            if rows is not None:
                payload['rows'] = rows
            content = self.osti_request(payload=payload)
            page_size = int(content['@rows'])
            if start_record == 0:
                num_records_total = int(content['@numfound'])
                pbar = tqdm(total=num_records_total)
                remaining_num_records = num_records_total - page_size
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
                if record['doi']['@status'] == 'COMPLETED':
                    doc['validated_on'] = datetime.combine(date.today(), datetime.min.time())
                    doc['doi'] = record['doi']['#text']
                doi_docs.append(doc)
            num_records = len(self.doi_store.insert(doi_docs))
            pbar.update(num_records)
        pbar.close()
        logger.info('all DOIs pulled from E-Link and inserted into doicoll')

    def _date_range_group_cond(self, dates):
        return {'$cond': [
            {'$lte': ['$date', dates[0] + timedelta(1)]}, dates[0],
            self._date_range_group_cond(dates[1:])
        ]} if dates else datetime.now()

    def get_traces(self):
        from plotly.graph_objs import Scatter
        import colorlover as cl
        colors = cl.scales['5']['qual']['Set1']
        traces = [
            Scatter(x=[], y=[], line=dict(color=(colors[0])), name='total materials'),
            Scatter(x=[], y=[], line=dict(color=(colors[1])), name='total requested DOIs'),
            Scatter(x=[], y=[], line=dict(color=(colors[2])), name='total validated DOIs'),
            Scatter(x=[], y=[], line=dict(color=(colors[3])), name='total bibtex-ed DOIs'),
            Scatter(x=[], y=[], line=dict(color=(colors[4])), name='total built DOIs'),
            Scatter(x=[], y=[], line=dict(dash='dot', color=(colors[0])), name='new materials'),
            Scatter(x=[], y=[], line=dict(dash='dot', color=(colors[1])), name='newly requested DOIs'),
            Scatter(x=[], y=[], line=dict(dash='dot', color=(colors[2])), name='newly validated DOIs'),
            Scatter(x=[], y=[], line=dict(dash='dot', color=(colors[3])), name='newly bibtex-ed DOIs'),
            Scatter(x=[], y=[], line=dict(dash='dot', color=(colors[4])), name='newly built DOIs'),
        ]
        num_requested_dois = 0
        for doc in self.doi_store.aggregate([
            {'$group': {'_id': '$created_on', 'num': {'$sum': 1}}},
            {'$sort': {'_id': 1}},
        ]):
            num_requested_dois += doc['num']
            date = doc['_id'].date()
            traces[6].x.append(date)
            traces[6].y.append(doc['num'])
            traces[1].x.append(date)
            traces[1].y.append(num_requested_dois)
        num_validated_dois = 0
        for doc in self.doi_store.aggregate([
            {'$match': {'doi': {'$exists': True}}},
            {'$group': {'_id': '$validated_on', 'num': {'$sum': 1}}},
            {'$sort': {'_id': 1}},
        ]):
            num_validated_dois += doc['num']
            date = doc['_id'].date()
            traces[7].x.append(date)
            traces[7].y.append(doc['num'])
            traces[2].x.append(date)
            traces[2].y.append(num_validated_dois)
        num_bibtexed_dois = 0
        for doc in self.doi_store.aggregate([
            {'$match': {
                'doi': {'$exists': True}, 'bibtex': {'$exists': True},
            }},
            {'$group': {'_id': '$bibtexed_on', 'num': {'$sum': 1}}},
            {'$sort': {'_id': 1}},
        ]):
            num_bibtexed_dois += doc['num']
            date = doc['_id'].date()
            traces[8].x.append(date)
            traces[8].y.append(doc['num'])
            traces[3].x.append(date)
            traces[3].y.append(num_bibtexed_dois)
        num_built_dois = 0
        for doc in self.doi_store.aggregate([
            {'$match': {
                'doi': {'$exists': True}, 'bibtex': {'$exists': True},
                'built_on': {'$exists': True}
            }},
            {'$group': {'_id': '$built_on', 'num': {'$sum': 1}}},
            {'$sort': {'_id': 1}},
        ]):
            num_built_dois += doc['num']
            date = doc['_id'].date()
            traces[9].x.append(date)
            traces[9].y.append(doc['num'])
            traces[4].x.append(date)
            traces[4].y.append(num_built_dois)
        dates = [datetime.combine(d, datetime.min.time()) for d in traces[1].x]
        nmats = {
            doc['_id']: doc['num']
            for doc in self.materials_store.aggregate([
                {'$group': {
                    '_id': self._date_range_group_cond(dates),
                    'num': {'$sum': 1}
                }}
            ])
        }
        num_materials = 0
        for dt in dates:
            num = nmats[dt] if dt in nmats else 0
            num_materials += num
            date = dt.date()
            traces[5].x.append(date)
            traces[5].y.append(num)
            traces[0].x.append(date)
            traces[0].y.append(num_materials)
        return traces

    def get_materials_cursor(self, num_or_list):
        if isinstance(num_or_list, int) and num_or_list > 0:
            existent_mpids = self.doi_store.find().distinct('_id')
            return self.materials_store.find({
                'doi': {'$exists': False}, 'sbxn': 'core',
                'task_id': {'$nin': existent_mpids}
            }, limit=num_or_list)
        elif isinstance(num_or_list, list) and len(num_or_list) > 0:
            mp_ids = [el if 'mp' in el else 'mp-'+el for el in num_or_list]
            return self.materials_store.find({'task_id': {'$in': mp_ids}})
        else:
          logger.error('cannot get materials cursor from {}'.format(num_or_list))
          return []

    def get_duplicate(self, mpid):
        dup = self.duplicates.get(mpid)
        if dup is None:
            logger.error('missing DOI for duplicate {}! DB reset?'.format(mpid))
            sys.exit(1)
        dup['created_on'] = datetime.combine(dup['created_on'], datetime.min.time())
        dup['updated_on'] = dup['created_on']
        dup['validated_on'] = dup['created_on']
        logger.info('found DOI {} for {} in dup-file'.format(dup['doi'], mpid))
        return dup

    def get_osti_id(self, mpid):
        # empty osti_id = new submission -> new DOI
        # check for existing doi to distinguish from edit/update scenario
        doi_entry = self.doi_store.find_one({'_id': mpid})
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
                    doc['created_on'] = datetime.combine(
                        date.today(), datetime.min.time()
                    )
                    doc['updated_on'] = doc['created_on']
                    if 'doi' in doc:
                        doc['validated_on'] = doc['created_on']
                dois_insert.append(doc)
        if dois_insert:
            docs_inserted = self.doi_store.insert(dois_insert)
            logger.debug('{} DOIs inserted'.format(len(docs_inserted)))
        if dois_update:
            ndocs_updated = self.doi_store.update(
                {'_id': {'$in': dois_update}},
                {'$set': {'updated_on': datetime.now()}},
                multi=True
            )['nModified']
            logger.debug('{} DOI docs updated'.format(ndocs_updated))
