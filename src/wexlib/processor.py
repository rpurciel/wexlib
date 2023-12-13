# class ProductNotValidException(Exception):

# 	def __init__(self, product):
# 		self.message = f"Product '{product}' is not a valid product to initalize or has not been implemented"

# 	def __str__(self):
# 		return self.message

# class BBOX:

# 	def __init__(self):

import os
import json
import datetime
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from multiprocessing import Pool, RLock, freeze_support
from random import random
from threading import RLock as TRLock
import uuid

import s3fs
import boto3
import botocore
import botocore.config as botoconfig
from tqdm.auto import tqdm
from tqdm.contrib.concurrent import process_map, thread_map
import pandas as pd

#import wexlib.util as util

md_file = open(os.path.join(os.path.dirname(os.getcwd()), "wexlib/ancillary/productmetadata.json"))
product_metadata = json.load(md_file)

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
		self.file_ext = product_info['file_extension']
		self.run_period = product_info['run_period']

		self.storage_dir = os.path.join(self.working_dir, "Data", self.internal_name)
		if not os.path.exists(self.storage_dir):
			os.makedirs(self.storage_dir)

		if self.storage_type == 'aws':
			self.aws_bucket_id = product_info['aws_bucket_id']
			self.aws_key_patterns = product_info['aws_key_patterns']

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

		self.catalog = pd.DataFrame(columns=['file', ])

		if self.storage_type == 'aws':
			self.aws_bucket_id = parent_class.aws_bucket_id
			self.aws_key_patterns = parent_class.aws_key_patterns

			self.s3fs_client = None
			self.boto3_session = None
			self.boto3_client = None

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

	## Downloading methods: AWS

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

	def _load_files_from_directory(self, path):
		return sorted(glob.glob(path + "/" + self.file_ext))

	def parse_static_files(self, path):
		pass

class ModelProcessor(_ProcessorGeneric):

	def archive_download(self, 
						 start_time: datetime.datetime, 
						 end_time: datetime.datetime, 
						 *args,
						 **kwargs):
		'''
		Download data from an archive.

		Default mode of download is to download analysis model
		files with a period equal to the model run period.
		(e.g. 24 hours of GFS would return 4 analysis files,
		for 0, 6, 12, and 18z). Different modes can be specified via flags.
		'''

		selected_version = self.product_versions[0]

		if 'download_as_forecast' in kwargs:
			dl_fcst = kwargs.get('download_as_forecast')

		#user selected period is coming later LOL

		# if 'period' in kwargs:
		# 	def_period = kwargs.get('period')

		# 	if pandas.PeriodDtype(freq=def_period).freq != pandas.PeriodDtype(freq=self.run_period).freq:
		# 		if dl_fcst:
		# 			#Case FXX: Forecast data is downloaded, up to length of forecast.
		# 			period = pandas.period_range(start=start_time, end=end_time, freq=def_period)
		# 		else:
		# 			#Case ANL(x): analysis data is preferred, filled with forecast data when analysis data is
		# 			#not available. Filling only happens if input period < run period.
		# 			period = pandas.period_range(start=start_time, end=end_time, freq=def_period)
		# 			fill_with_fcst = True
		# 			dl_fcst = False
		# else:
		# 	#CASE FXX: Forecast data is downloaded, up to length of forecast.
		# 	#CASE ANL(b): Only analysis data is downloaded.
		# 	#difference is decided at runtime based on dl_fcst flag above.
		# 	period = pandas.period_range(start=start_time, end=end_time, freq=self.run_period)

		period = pd.period_range(start=start_time, end=end_time, freq=self.run_period)

		keys = []
		file_names = []
		for time in period:
			file_uuid = str(uuid.uuid4())

			year = str(time.year).zfill(4)
			month = str(time.month).zfill(2)
			day = str(time.day).zfill(2)
			hour = str(time.hour).zfill(2)
			product = "conus"
			fcst_hr = "00"

			# floored_to_anl = time.floor(freq=self.run_period)
			# start_floored_to_anl = start_time.floor(freq=self.run_period)
			# time_offset_from_start = round((time - start_time).total_seconds())
			# time_offset_from_start_anl = round((time - start_floored_to_anl).total_seconds())
			# time_offset_from_anl_before_start = round((time - start_time.floor(freq=self.run_period)).total_seconds())
			# on_anl_time = lambda x: True if x % self.run_period_sec == 0 else False

			# if on_anl_time(time_offset):
			# 	if fill_with_fcst and not dl_fcst:
			# 		#CASE ANL(x) on analysis time
			# 		year = str(time.year).zfill(4)
			# 		month = str(time.month).zfill(2)
			# 		day = str(time.day).zfill(2)
			# 		hour = str(time.hour).zfill(2)
			# 		fcst_hr = "anl"
			# 	elif dl_fcst and not fill_with_fcst:
			# 		#CASE FXX
			# 		year = str(start_time.year).zfill(4)
			# 		month = str(start_time.month).zfill(2)
			# 		day = str(start_time.day).zfill(2)
			# 		hour = str(start_time.hour).zfill(2)
			# 		fcst_hr = "anl"


			# else:
			# 	#This will never be entered for CASE ANL(b)
			# 	if fill_with_fcst:
			# 		#CASE ANL(x) off analysis time
			# 		year = str(floored_to_anl.year).zfill(4)
			# 		month = str(floored_to_anl.month).zfill(2)
			# 		day = str(floored_to_anl.day).zfill(2)
			# 		hour = str(floored_to_anl.hour).zfill(2)
			# 		hrs_from_start = time_offset_from_start / 3600
			# 		fcst_hr = f"f{hrs_from_start.zfill(3)}"
			# 	elif dl_fcst and not fill_with_fcst:
			# 		#CASE FXX 
			# 		year = str(start_time.year).zfill(4)
			# 		month = str(start_time.month).zfill(2)
			# 		day = str(start_time.day).zfill(2)
			# 		hour = str(start_time.hour).zfill(2)
			# 		hrs_from_start = time_offset_from_start / 3600
			# 		fcst_hr = f"f{hrs_from_start.zfill(3)}"
				


			#TODO: Add in code to check if period is on a runtime interval,
			#for discernment between 
			
			key_pattern = self.aws_key_patterns[selected_version].format(**locals())
			if self._aws_file_exists(self.aws_bucket_id, key_pattern):
				keys += [key_pattern]
				file_name = f"{self.internal_name}{product}.{year}{month}{day}{hour}.f{fcst_hr}.{file_uuid}{self.file_ext}"
				file_names += [file_name]

		self._create_boto3_session()

		self._aws_download_multithread(self.storage_dir, 
		                               self.aws_bucket_id, 
		                               keys, file_names)

		#self._load_into_catalog(file_names,)

		print("All files finished downloading.")

	def _open_file(self):
		pass

	def svar(self, vars):
		pass


def create(storage_dir_path, product):
	return _ProcessorInstantiator(storage_dir_path, product)

if __name__ == "__main__":

	# new_proc = create('/Users/rpurciel/Development/NON GH/test', "gfs")

	start_time = datetime.datetime(2023, 9, 2, 0, 0)
	end_time = datetime.datetime(2023, 9, 3, 0, 0)

	# new_proc.archive_download(start_time, end_time)

	hrrr_proc = create('/Users/rpurciel/Development/NON GH/test', "hrrr")
	print(hrrr_proc)

	hrrr_proc.archive_download(start_time, end_time)

















