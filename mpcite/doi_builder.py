import logging, requests, pybtex, yaml, os, time
from datetime import datetime, timedelta, date
from xmltodict import parse
from maggma.core.builder import Builder
import sys
from maggma.stores import Store
from pathlib import Path
from utility import DictAsMember
from adapter import OstiMongoAdapter
from typing import Iterable, List, Union, Tuple
from utility import OSTI


class DoiBuilder(Builder):
    """
    A builder to combine information from the Materials Project database(MP) and Osti Explorer(OSTI) to produce a *dois*
    collections

    *dois* collection is similar to a "cache" where it maps the mp ids that are in MP to the DOIs that are in the OSTI

    This builder will find all materials that are in MP but are not in the  in the *dois* collection, call this list of
    uncited_materials

    It will submit the uncited_materials to OSTI and constantly check whether OSTI has updated its database

    if it finds one of the uncited_materials in OSTI, it will update the *dois* collection

    """

    def __init__(self, adapter: OstiMongoAdapter, osti: OSTI, **kwargs):
        """
         connection with materials database
            1. establish connection with materials collection (Guaranteed online)
            2. establish connection with doi collection (online or local)
            3. establish connection with robocrys (Guaranteed online)

        establish connection with ELink to submit info

        establish connection with osti explorer to get bibtex
        :param adapter: OstiMongoAdapter that keeps track of materials, doi, and other related stores
        :param config: configuration dictionary
        :param kwargs: other keywords fed into Builder(will be documented as development goes on)
        """
        self.adapter = adapter
        super().__init__(sources=[adapter.materials_store, adapter.robocrys_store],
                         targets=[adapter.doi_store],
                         **kwargs)
        self.num_bibtex_errors = 0
        self.osti = osti
        self.logger.debug("DOI Builder Succesfully instantiated")

    def get_items(self) -> Iterable:
        """
        Gets all materials that need a DOI
            1. compare list of materials that in the materials collection against the ones in the DOI collection
            2. find and return the ones that needs update. Return the ones that satisfy the following conditions
                1. material that is in the material collection, but is NOT in the DOI collection
                2. valid is False
                3. condition (1) and (2) does not satisfy, but in the DOI collection, DOI doesn't exist
                4. condition (1) and (2) does not satisfy, but in the DOI collection, DOI bibtext does not exist
                5. condition (1) and (2) does not satisfy, but in the DOI collection, _status is not COMPLETED


        Note that 2.2 happen when something bad happened, like a material got moved or something,
        so that DOI entry needs to be updated

        Note that 2.3 happens when MPCite submitted a material to ELink and is waiting to hear back

        Note that 2.4 happens when MPCite submitted a material to ELink, queried OSTI, and does not find anything,
        therefore needs to constantly reachout to ELINK and OSTI for update

        Returns:
            generator of materials to retrieve/build DOI
        """
        self.logger.info("DoiBuilder Get Item Started")
        # TODO: ask patrick if he can come up with better query
        materials = set(self.adapter.materials_store.distinct(self.adapter.materials_store.key))
        dois = set(self.adapter.doi_store.distinct(self.adapter.doi_store.key))
        to_add = list(materials - dois)
        self.logger.debug(f"There are {len(to_add)} records that are in the materials collection but are not in "
                          f"the DOIs collection")
        query = {
            '$or': [{'valid': False},
                    {'doi': {'$exists': False}},
                    {'bibtex': {'$exists': False}},
                    {'_status': {'$ne': 'COMPLETED'}}]
        }
        to_update = self.adapter.doi_store.distinct(self.adapter.doi_store.key, query)
        self.logger.debug(f"There are {len(to_update)} records that are already in DOI collection but needs to be "
                          f"updated")  # TODO the doi collection i was given is different than the test elink database
        overall = to_add + to_update
        # overall = to_update
        self.logger.info(f"Overall, There are {len(overall)} records that needs to be updated")
        return overall

    def process_item(self, item: str) -> dict:
        """
        build current document with DOI info
        Args:
            item (str): taskid/mp-id of the material
        Returns:
            dict: a DOI dict
        """
        material_id = item
        self.logger.info("Processing document with task_id = {}".format(material_id))
        doi_doc = {self.adapter.materials_store.key: material_id}

        # validate DOI
        time.sleep(.5)
        result = self.get_doi_from_elink(material_id)
        if result is None:
            try:
                self.post_data_to_elink({})
            except Exception as e:
                self.logger.error(f"posting failed due to {e}")
        else:
            doi, status = result
            self.logger.debug('{}: {} ({}) received from E-Link'.format(material_id, doi, status))

            # need send a new request
        # doi_doc.update({'doi': doi, 'status': status, 'valid': False})
        # ready = bool(status == 'COMPLETED' or (status == 'PENDING' and doi))
        # if ready and self.num_bibtex_errors < 3:
        #     try:
        #         doi_doc['bibtex'] = self.get_bibtex(doi)
        #         doi_doc['valid'] = True
        #     except ValueError:
        #         self.num_bibtex_errors += 1

        # TODO record generation and submission
        # if not rec.generate(num_or_list):
        #    return
        # rec.submit()
        return doi_doc

    def update_targets(self, items):
        self.logger.info("No items to update")

    ############ utilities ###################
    class MultiValueFoundOnELinkError(Exception):
        pass

    class HTTPError(Exception):
        pass

    def post_data_to_elink(self, data):
        """

        :param data: data to post, gaurenteed in xml format
        :return:
        """
        self.logger.info("Posting data to elink")
        auth = (self.osti.elink.username, self.osti.elink.password)
        r = requests.post(self.osti.elink.endpoint, auth=auth, data=data)
        # TODO walk through the submit function with patrick
        # https://github.com/materialsproject/MPCite/blob/next_gen/mpcite/record.py#L115
        pass

    def get_doi_from_elink(self, mpid_or_ostiid: str) -> Union[None, Tuple[str, str]]:
        """
        Get DOI from E-link
        If nothing is returned, that means E-Link does not have that record yet, return None
        raise error if
            - found multiple entries on e-link
            - got an error code that is not 200

        :param mpid_or_ostiid:
        :return:
            None if E-Link returned nothing
            otherwise a tuple of (DOI, status), ex: (10.5072/1322571, COMPLETED)
            Note, E-link changed captilization,
            for MPCite, we are ALWAYS going to capitalize the status for consistency
        """
        key = 'site_unique_id' if 'mp-' in mpid_or_ostiid or 'mvc-' in mpid_or_ostiid else 'osti_id'
        payload = {key: mpid_or_ostiid}
        auth = (self.osti.elink.username, self.osti.elink.password)

        self.logger.debug('GET from {} w/i payload = {} ...'.format(self.osti.elink.endpoint, payload))

        r = requests.get(self.osti.elink.endpoint, auth=auth, params=payload)
        if r.status_code == 200:
            content = parse(r.content)
            if int(content["records"]["@numfound"]) == 1:
                doi = content["records"]["record"]["doi"]
                return doi["#text"], doi["@status"]
            if int(content["records"]["@numfound"]) == 0:
                return None
            else:
                msg = f"Multiple records for {mpid_or_ostiid} is found"
                self.logger.error(msg)
                raise DoiBuilder.MultiValueFoundOnELinkError(msg)
        else:
            msg = f"Error code from GET is {r.status_code}"
            self.logger.error(msg)
            raise DoiBuilder.HTTPError(msg)

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

# class DoiCopyBuilder(CopyBuilder):
#
#     def process_item(self, item):
#         doc = {'_id': item['_id'], 'task_id': item['_id']}
#         doc['valid'] = 'validated_on' in item
#         for k in ['doi', 'bibtex', 'last_updated']:
#           doc[k] = item.get(k)
#         return doc
