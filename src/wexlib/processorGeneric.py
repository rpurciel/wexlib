import os
import glob
import json
from threading import RLock as TRLock
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from multiprocess import Pool, RLock, freeze_support
import s3fs
import boto3
import botocore
import botocore.config as botoconfig
from tqdm.auto import tqdm
from tqdm.contrib.concurrent import process_map, thread_map

class _ProcessorGeneric():
    '''
    Parent class type for all processor types. Implements printing,
    iterating, and functions generic to all processors.
    '''

    def __init__(self, parent_class):

        parent_class.__init__(self, parent_class.working_dir, parent_class.product_name)

        # self.product_name = parent_class.product_name
        # self.long_name = parent_class.long_name

        # self.category = parent_class.category
        # self.internal_name = parent_class.internal_name
        # self.product_id = parent_class.product_id

        # self.storage_type = parent_class.storage_type
        # self.product_versions = parent_class.product_versions
        # self.storage_dir = parent_class.storage_dir
        # self.file_ext = parent_class.file_ext
        # self.run_period = parent_class.run_period

        if self.storage_type == 'aws':
            self.aws_bucket_id = parent_class.aws_bucket_id
            self.aws_key_patterns = parent_class.aws_key_patterns

            self.s3fs_client = None
            self.boto3_session = None
            self.boto3_client = None
        elif self.storage_type == 'cds':
            self.cds_uid = parent_class.cds_uid
            self.cds_api_key = parent_class.cds_api_key
            self.cds_base_url = parent_class.cds_base_url
            self.cds_sources = parent_class.cds_sources

        #self._stored_figure = None

    def __str__(self):
        return "hello"

    def __iter__(self):
        if not self.files:
            self.files = self._load_files_from_directory(self.storage_dir)
        self.file_idx = 0
        return self

    def __next__(self):
        if self.file_idx <= len(self.files):
            file_path = self.files[self.file_idx]
            self.file_idx += 1
            return file_path
        else:
            del self.files #Delete file list to clear memory (needed? idk)
            raise StopIteration

    ### Catalog Functions

    def _load_catalog(self) -> dict:
        with open(self.catalog_path) as catalog:
            return json.load(catalog)

    def _save_catalog(self, data: dict):
        with open(self.catalog_path, 'w') as catalog:
            json.dump(data, catalog, indent=2)

    def _add_catalog_chunk(self, category, chunk):

        catalog = self._load_catalog()

        chunks_list = catalog[category]

        if not chunks_list:
            chunks_list = [chunk,]
        else:
            chunks_list += [chunk,]

        catalog.update({category : chunks_list})

        self._save_catalog(catalog)

    def _select_data_chunk(self, category, chunk_uuid):

        catalog = self._load_catalog()
        print(catalog[category][0]['chunk_uuid'])
        for chunk in catalog[category]:
            if chunk_uuid == chunk['chunk_uuid']:
                return chunk


    ### Downloading methods: AWS

    def _create_boto3_session(self):
        if not self.boto3_session:
            self.boto3_session = boto3.Session()

        if not self.boto3_client:
            self.boto3_client = self.boto3_session.resource("s3", config=botoconfig.Config(signature_version=botocore.UNSIGNED))

    def _list_aws_keys(self, 
                       bucket: str, 
                       key_pattern: str,
                       glob_match=False):
        '''
        Function to find valid s3 files based on a bucket name and a key pattern.
        Allows for unix-style glob matching.

        Parameters
        ----------
        bucket  : str
            Bucket name to use.
        key_pattern  : str
            Key pattern string to use. If glob_match is False, must be a
            path to a folder containing files (not subfolders), otherwise
            nothing will be downloaded.
            If glob_match is True, uses standard terminology.
        glob_match  : bool, optional [default: False]
            Turns on glob-style matching for key names.

        Returns
        -------
        out  : A list of valid file keys for the given bucket.
        '''
        if not self.s3fs_client:
            self.s3fs_client = s3fs.S3FileSystem(anon=True)

        if not glob_match:
            return [key.replace(f"{bucket}/", "") for key in self.s3fs_client.ls(f"{bucket}/{key_pattern}")]
        else:
            return [key.replace(f"{bucket}/", "") for key in self.s3fs_client.glob(f"{bucket}/{key_pattern}")]

    def _aws_file_exists(self,
                         bucket: str,
                         key_head: str):

        if not self.s3fs_client:
            self.s3fs_client = s3fs.S3FileSystem(anon=True)

        return self.s3fs_client.exists(f"{bucket}/{key_head}")

    
    def _aws_download_multithread(self, save_dir, bucket, keys, file_names):
        '''
        Thin wrapper for multithreaded downloading.
        '''

        tqdm.set_lock(TRLock())
        try:
            with ThreadPoolExecutor(initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),)) as executor:
                executor.map(partial(self._aws_download_multithread_worker, save_dir, bucket, self.boto3_client), keys, file_names, range(1, len(keys)+1, 1))
        except Exception as e:
            print(e)

    def _aws_download_multithread_worker(self,
                             save_dir: str, 
                             bucket: str,
                             boto3: boto3.resource, 
                             s3_file: str,
                             file_name: str, 
                             progress_pos: int):

        file_size = int(boto3.Object(bucket, s3_file).content_length)
        pretty_file_name = os.path.basename(s3_file)
        file_path = os.path.join(save_dir, file_name)

        try:
            with tqdm(unit='B', unit_scale=True, unit_divisor=1024, miniters=1, desc=pretty_file_name, total=file_size, leave=None, position=progress_pos) as progress:
                boto3.Bucket(bucket).download_file(s3_file, file_path, Callback=progress.update)
                progress.close()    
                progress.display(f"{pretty_file_name}: Finished downloading.", pos=progress_pos)
        except Exception as e:
            print(e)


    ### Downloading Methods: CDS

    def _cds_download_multithread(self, save_dir, requests, file_names):

        tqdm.set_lock(TRLock())
        try:
            with ThreadPoolExecutor(initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),)) as executor:
                executor.map(partial(self._cds_download_multithread_worker, save_dir), requests, file_names, range(1, len(requests)+1, 1))
        except Exception as e:
            print(e)

    def _cds_download_multithread_worker(self,
                                         save_dir: str,
                                         cds_source: str, 
                                         request: dict,
                                         file_name: str, 
                                         progress_pos: int):

        file_path = os.path.join(save_dir, file_name)

        try:
            with tqdm(desc=file_name, leave=None, position=progress_pos) as progress:
                progress.display(f"{file_name}: Queueing request...", pos=progress_pos)
                request_id = self._cds_queue_request(cds_source, request)
                progress.display(f"{file_name}: Request queued, checking if finished...", pos=progress_pos)

                while True:
                    status, file_url = self._cds_check_if_request_finished(cds_source, request_id)
                    if not status:
                        # progress.set_description(f"{file_name}: Still queued, retrying in...", pos=progress_pos)
                        with tqdm(desc=f"{file_name}", bar_format='{desc} |{bar}|',total=30) as prog:
                            for i in range(0, 30, 1):
                                prog.update()
                                prog.set_description(f'{file_name}: Retrying in ' + tqdm.format_interval(30-i))
                                time.sleep(1)
                    else:
                        if file_url == 'failed':
                            progress.set_description(f"{file_name}: Error - Request Failed (at CDS endpoint)", pos=progress_pos)
                            break
                        else:
                            progress.set_description(f"{file_name}: Request completed, downloading...", pos=progress_pos)
                            self._cds_download_request(file_url, dest_path, progress)
                            break

        except Exception as e:
            print(e)

    def _cds_queue_request(self, cds_source, request_id):

        queue_request = requests.post(f"{self.cds_base_url}/resources/{cds_source}", auth=(self.cds_uid, self.cds_api_key), data=json.dumps(request))
        return queue_request.json()['request_id']

    def _cds_check_if_request_finished(self, request_id):

        inquiry_request = requests.get(f"{self.cds_base_url}/tasks/{request_id}", auth=(self.cds_uid, self.cds_api_key))
        inquiry = inquiry_request.json()

        if inquiry['state'] == 'completed':
            return True, inquiry['location']
        elif inquify['state'] == 'failed':
            return False, 'failed'
        else:
            return False, None

    def _cds_download_request(self, target_url, dest_path, progress_obj):

        download_stream = requests.get(target_url, stream=True)
        with open(dest_path, 'wb') as dest_file:
          for chunk in download_stream.iter_content(chunk_size=128):
              dest_file.write(chunk)
              progress_obj.update(len(chunk))

    def _load_files_from_directory(self, path):
        return sorted(glob.glob(path + "/" + self.file_ext))

    def parse_static_files(self, path):
        for file in self._load_files_from_directory(path):
            print(path)

    def load_chunk(self, chunk_uuid: str):
        self.selected_chunk = self._select_data_chunk("model", chunk_uuid)

