import os
import json

from modelProcessor import ModelProcessor
# from satProcessor import SatProcessor
# from radarProcessor import RadarProcessor
# from obsProcessor import SurfaceProcessor
# from miscProcessor import OtherProcessor

MD_FILE = open(os.path.join(os.path.dirname(os.getcwd()), "wexlib/ancillary/productmetadata.json"))
product_metadata = json.load(MD_FILE)

class _ProcessorInstantiator():
    '''
    Hidden class type to select what type of processor object to create
    based on what product was passed.
    '''

    def __new__(self, working_dir_path, product):

        self.__init__(self, working_dir_path, product)

        if self.category == "model":
            return ModelProcessor(self)
        elif self.category == "sat":
            return SatProcessor(self)
        elif self.category == "radar":
            return RadarProcessor(self)
        elif self.category == "sfc":
            return SurfaceProcessor(self)
        elif self.category == "misc":
            return OtherProcessor(self)
        else:
            raise AttributeError("Unknown product category")

    def __init__(self, working_dir_path, product_alias):

        self.working_dir = working_dir_path
        if not os.path.exists(self.working_dir):
            os.makedirs(self.working_dir)

        product_info = [prod['info'] for prod in product_metadata if product_alias in prod['alias']]
        if not product_info:
            raise NotImplementedError(f"Name '{product_alias}' not recognized or product not implemented")
        else:
            product_info = product_info[0]

        self.product_name = product_info['product_name']
        self.long_name = product_info['long_name']

        self.category = product_info['category']
        self.internal_name = product_info['internal_name']
        self.product_id = product_info['id']

        self.storage_type = product_info['storage_type']
        self.product_versions = product_info['product_versions']
        self.data_format = product_info['data_format']
        self.file_ext = product_info['file_extension']
        self.run_period = product_info['run_period']
        self.data_format = product_info['data_format']

        self.sounding_params = product_info['sounding_parameters']

        self.storage_dir = os.path.join(self.working_dir, "Data", self.internal_name)
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)

        if self.storage_type == 'aws':
            self.aws_bucket_id = product_info['aws_bucket_id']
            self.aws_key_patterns = product_info['aws_key_patterns']
        elif self.storage_type == 'cds':
            self.cds_uid = product_info['cds_uid']
            self.cds_api_key = product_info['cds_api_key']
            self.cds_base_url = product_info['cds_base_url']
            self.cds_sources = product_info['cds_sources']

        self.catalog_path = os.path.join(self.working_dir, "Data", "catalog.json")
        if not os.path.exists(self.catalog_path):
            self.catalog = {
                'model': [],
                'satellite': [],
                'radar': [],
                'others': []
            }
            with open(self.catalog_path, 'w') as catalog_file:
                json.dump(self.catalog, catalog_file, indent=2)

        self.selected_chunk = None