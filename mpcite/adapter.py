import logging
from maggma.core import Store
from maggma.stores import MongoStore
from models import ELinkGetResponseModel, RoboCrysModel

logger = logging.getLogger('mpcite')


class OstiMongoAdapter(object):
    """adapter to connect to materials database and collection"""

    def __init__(self, materials_store: Store, dois_store: Store, robocrys_store: Store):
        """

        :param materials_store: represent a connection to the materials store
        :param dois_store: represent a connection to the dois store
        :param robocrys_store: represent a connection to the robocrys store
        """
        self.materials_store: Store = materials_store
        self.doi_store: Store = dois_store
        self.robocrys_store: Store = robocrys_store

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

        # duplicates_file = os.path.expandvars(config.duplicates_file)
        # duplicates = loadfn(duplicates_file) if os.path.exists(duplicates_file) else {}

        return OstiMongoAdapter(materials_store=materials_store,
                                dois_store=dois_store,
                                robocrys_store=robocrys_store)

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
                          key=config[config_collection_name]["key"] if "key" in config[config_collection_name] else
                          "task_id")

    def get_material_description(self, mp_id: str) -> str:
        """
        find materials description from robocrys database, if not found return the default description

        :param mp_id: mp_id to query for in the robocrys database
        :return:
            description in string
        """
        robo_result = self.robocrys_store.query_one(criteria={self.robocrys_store.key: mp_id})
        if robo_result is None:
            return ELinkGetResponseModel.get_default_description()
        else:
            robo_result = RoboCrysModel.parse_obj(robo_result)
            return robo_result.description

    def get_osti_id(self, mp_id) -> str:
        """
        Used to determine if an update is necessary.

        If '' is returned, implies update is not necessary.

        Otherwise, an update is necessary
        :param mp_id:
        :return:
        """
        doi_entry = self.doi_store.query_one(criteria={self.doi_store.key: mp_id})
        if doi_entry is None:
            return ''
        else:
            return doi_entry['doi'].split('/')[-1]

    def clear_doi_store(self, criteria=None):
        if criteria is None:
            criteria = {}
        self.doi_store.remove_docs(criteria=criteria)
