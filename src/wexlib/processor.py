
import os
import json
from datetime import datetime
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from functools import partial
#from multiprocessing import Pool, RLock, freeze_support
from random import random
from threading import RLock as TRLock
import uuid

from multiprocess import Pool, RLock, freeze_support
import s3fs
import boto3
import botocore
import botocore.config as botoconfig
from tqdm.auto import tqdm
from tqdm.contrib.concurrent import process_map, thread_map
import pandas as pd
import xarray as xr
import numpy as np
import metpy
from metpy.units import units
import metpy.calc as mpcalc

# class ProductNotValidException(Exception):

#     def __init__(self, product):
#         self.message = f"Product '{product}' is not a valid product to initalize or has not been implemented"

#     def __str__(self):
#         return self.message

# class BBOX:

#     def __init__(self):

class PointPath():

    def __init__(self, **kwargs):

        if "from_csv" in kwargs:
            path = kwargs.get("from_csv")

            with open(path, 'r') as csv_file:
                tag_str = csv_file.readlines(1)

            tag_str = tag_str[0]

            self.tags = {}

            for tag in tag_str.split(','):
                print(tag)
                if '#' in tag:
                    pass
                elif '=' in tag:
                    tag_key, tag_val = tag.replace('\n','').split('=')
                    self.tags.update({tag_key : tag_val})
                else:
                    pass

            self.path_df = pd.read_csv(path, 
                                       comment='#')


        self.lats = self.path_df['lat']
        self.lons = self.path_df['lon']
        self.alts = self.path_df['alt']
        self.names = self.path_df['name']

        if "time_mode" in self.tags:
            time_mode = self.tags.get("time_mode")
            if time_mode == 'comp':
                raise NotImplementedError
            if time_mode == 'str':
                self.times = self.path_df['time']

    def __get__(self, instance, owner):
        return self.value

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

    def _load_files_from_directory(self, path):
        return sorted(glob.glob(path + "/" + self.file_ext))

    def parse_static_files(self, path):
        pass


class ModelProcessor(_ProcessorGeneric):

    def archive_download(self, 
                         start_time: datetime, 
                         end_time: datetime, 
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
        #     def_period = kwargs.get('period')

        #     if pandas.PeriodDtype(freq=def_period).freq != pandas.PeriodDtype(freq=self.run_period).freq:
        #         if dl_fcst:
        #             #Case FXX: Forecast data is downloaded, up to length of forecast.
        #             period = pandas.period_range(start=start_time, end=end_time, freq=def_period)
        #         else:
        #             #Case ANL(x): analysis data is preferred, filled with forecast data when analysis data is
        #             #not available. Filling only happens if input period < run period.
        #             period = pandas.period_range(start=start_time, end=end_time, freq=def_period)
        #             fill_with_fcst = True
        #             dl_fcst = False
        # else:
        #     #CASE FXX: Forecast data is downloaded, up to length of forecast.
        #     #CASE ANL(b): Only analysis data is downloaded.
        #     #difference is decided at runtime based on dl_fcst flag above.
        #     period = pandas.period_range(start=start_time, end=end_time, freq=self.run_period)

        freq = self.run_period

        period = pd.period_range(start=start_time, end=end_time, freq=freq)

        keys = []
        file_names = []
        chunk_uuid = str(uuid.uuid4())
        chunk_entry = {
            "chunk_uuid": chunk_uuid,
            "id": self.product_id,
            "model_name": self.internal_name,
            "product": selected_version,
            "period_start": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "period_end": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "download_frequency": freq,
            "files": [],
            "errors": []
        }

        for time in period:
            file_uuid = str(uuid.uuid4())

            year = str(time.year).zfill(4)
            month = str(time.month).zfill(2)
            day = str(time.day).zfill(2)
            hour = str(time.hour).zfill(2)
            minute = str(time.minute).zfill(2)
            product = "conus"
            fcst_hr = "00" #f000 = gfs, 00 = hrrr

            tags = {}

            # floored_to_anl = time.floor(freq=self.run_period)
            # start_floored_to_anl = start_time.floor(freq=self.run_period)
            # time_offset_from_start = round((time - start_time).total_seconds())
            # time_offset_from_start_anl = round((time - start_floored_to_anl).total_seconds())
            # time_offset_from_anl_before_start = round((time - start_time.floor(freq=self.run_period)).total_seconds())
            # on_anl_time = lambda x: True if x % self.run_period_sec == 0 else False

            # if on_anl_time(time_offset):
            #     if fill_with_fcst and not dl_fcst:
            #         #CASE ANL(x) on analysis time
            #         year = str(time.year).zfill(4)
            #         month = str(time.month).zfill(2)
            #         day = str(time.day).zfill(2)
            #         hour = str(time.hour).zfill(2)
            #         fcst_hr = "anl"
            #     elif dl_fcst and not fill_with_fcst:
            #         #CASE FXX
            #         year = str(start_time.year).zfill(4)
            #         month = str(start_time.month).zfill(2)
            #         day = str(start_time.day).zfill(2)
            #         hour = str(start_time.hour).zfill(2)
            #         fcst_hr = "anl"


            # else:
            #     #This will never be entered for CASE ANL(b)
            #     if fill_with_fcst:
            #         #CASE ANL(x) off analysis time
            #         year = str(floored_to_anl.year).zfill(4)
            #         month = str(floored_to_anl.month).zfill(2)
            #         day = str(floored_to_anl.day).zfill(2)
            #         hour = str(floored_to_anl.hour).zfill(2)
            #         hrs_from_start = time_offset_from_start / 3600
            #         fcst_hr = f"f{hrs_from_start.zfill(3)}"
            #     elif dl_fcst and not fill_with_fcst:
            #         #CASE FXX 
            #         year = str(start_time.year).zfill(4)
            #         month = str(start_time.month).zfill(2)
            #         day = str(start_time.day).zfill(2)
            #         hour = str(start_time.hour).zfill(2)
            #         hrs_from_start = time_offset_from_start / 3600
            #         fcst_hr = f"f{hrs_from_start.zfill(3)}"
                


            #TODO: Add in code to check if period is on a runtime interval,
            #for discernment between 
            
            key_pattern = self.aws_key_patterns[selected_version].format(**locals())
            if self._aws_file_exists(self.aws_bucket_id, key_pattern):
                keys += [key_pattern]
                file_name = f"{self.internal_name}{product}.{year}{month}{day}{hour}.{fcst_hr}.{chunk_uuid}.{file_uuid}{self.file_ext}"
                file_names += [file_name]

                file_entry = {
                    "uuid": file_uuid,
                    "file_name": file_name,
                    "file_path": os.path.join(self.storage_dir, file_name),
                    "data_format": self.data_format,
                    "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "year": year,
                    "month": month,
                    "day": day,
                    "hour": hour,
                    "minute": minute,
                    "forecast_hour": fcst_hr,
                    "tags": tags
                }

                files_list = chunk_entry['files']
                if not files_list:
                    files_list = [file_entry,]
                else:
                    files_list += [file_entry,]
                chunk_entry.update({'files': files_list})

            else:

                error_entry = {
                    "what": "download.AWSNotFound",
                    "info": f"Bucket={self.aws_bucket_id}, Key={key_pattern}, Time={time.strftime('%Y-%m-%d %H:%M:%S')}"
                }
                errors_list = chunk_entry['errors']
                if not errors_list:
                    errors_list = [error_entry,]
                else:
                    errors_list += [error_entry,]
                chunk_entry.update({'errors': errors_list})

        self._create_boto3_session()

        self._aws_download_multithread(self.storage_dir, 
                                       self.aws_bucket_id, 
                                       keys, file_names)

        self.selected_chunk = chunk_entry
        self._add_catalog_chunk('model', chunk_entry)

        print("All files finished downloading.")

    def svar(self, vars):
        pass

    def sounding(self, product, *args):
        '''
        sounding('cross-section', Path)
        sounding('time-height', start_time, end_time, lat, lon)
        sounding('single', time, lat, lon)
        '''

        if self.selected_chunk == None:
            def_chunk = input("No data chunk selected, please input chunk UUID (from catalog.json): ")
            self.selected_chunk = self._select_data_chunk("model", def_chunk)

        if product == "cross-section":
            Path = args[0]

            self.used_points = []
            input_points = []

            for lat, lon, time, name in zip(Path.lats, Path.lons, Path.times, Path.names):

                use_point, selected_file = self._select_file_and_filter(lat, lon, time)

                if use_point:
                    this_point = (lat, lon, time, name, selected_file)
                    input_points += [this_point]

            tqdm.set_lock(RLock())
            p = Pool(initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),))
            p.starmap(partial(self._single_sounding_data), input_points)

        elif product == "time-height":
            start_time = args[0]
            end_time = args[1]
            lat = args[2]
            lon = args[3]
        elif product == "single":
            raise NotImplementedError
        else:
            raise NotImplementedError

    def _select_file_and_filter(self,
                                sounding_lat: float, 
                                sounding_lon: float, 
                                time: pd.Timestamp,
                                skip_duplicate_points=True,
                                **kwargs):
        """
        Generates a CSV for plotting in RAOB from input HRRR file.

        Inputs: Path to HRRR data file, directory to save output to,
                latitude and longitude for sounding. 

        CSV is saved to output directory with autogenerated name,
        based on input file. Only able to process a single time
        (the first time) from a single file.

        Returns: Success code, time (in seconds) for function to run,
                 path to output file
        """

        data_period_start = datetime.strptime(self.selected_chunk['period_start'], "%Y-%m-%d %H:%M:%S")
        data_period_end = datetime.strptime(self.selected_chunk['period_end'], "%Y-%m-%d %H:%M:%S")
        data_period_freq = self.selected_chunk['download_frequency']

        time_obj = pd.to_datetime(time)
        nearest_time = time_obj.round(freq=data_period_freq)

        if nearest_time > data_period_end:
            nearest_time = data_period_end

        if nearest_time < data_period_start:
            nearest_time = data_period_start

        if self.sounding_params['requires_lon_conversion'] == True and sounding_lon < 0:
            sounding_lon += 360

        data_files = self.selected_chunk['files']

        selected_file = {}
        for file in data_files:
            if pd.to_datetime(file['time']) == nearest_time:
                selected_file = file

        if not selected_file:
            raise ValueError("Data file for this time not found.")

        if selected_file['data_format'] == 'grib':
            engine = 'cfgrib'
        else:
            engine = 'netcdf4'

        if self.selected_chunk['model_name'] == "hrrr": #HRRR

            data = xr.open_dataset(selected_file['file_path'], 
                                 engine = engine,
                                 filter_by_keys = {'typeOfLevel': 'isobaricInhPa', 'shortName' : 't'},
                                 errors='ignore')

            abslat = np.abs(data.latitude-sounding_lat)
            abslon = np.abs(data.longitude-sounding_lon)
            c = np.maximum(abslon, abslat)
            ([idx_y], [idx_x]) = np.where(c == np.min(c))
            data_pt = data.sel(y=idx_y, x=idx_x)

        elif self.selected_chunk['model_name'] == "gfs":

            data = xr.open_dataset(selected_file['file_path'], 
                     engine = engine,
                     filter_by_keys = {'typeOfLevel': 'isobaricInhPa', 'shortName' : 't'},
                     errors='ignore')

            data_pt = data.sel(longitude=sounding_lon, latitude=sounding_lat, method='nearest')

        selected_point = (data_pt.latitude.data, data_pt.longitude.data)

        print(f"({sounding_lat}, {sounding_lon}), ", selected_point)
        if skip_duplicate_points:
            points_to_skip = self.used_points.copy()
            if selected_point in points_to_skip:
                return False, selected_file
            else:
                points_to_skip += [selected_point]
                self.used_points = points_to_skip
                return True, selected_file


    def _single_sounding_data(self,
                              sounding_lat: float, 
                              sounding_lon: float, 
                              time: pd.Timestamp,
                              sounding_name: str,
                              selected_file: dict,
                              **kwargs):
        """
        Generates a CSV for plotting in RAOB from input HRRR file.

        Inputs: Path to HRRR data file, directory to save output to,
                latitude and longitude for sounding. 

        CSV is saved to output directory with autogenerated name,
        based on input file. Only able to process a single time
        (the first time) from a single file.

        Returns: Success code, time (in seconds) for function to run,
                 path to output file
        """

        time_obj = pd.to_datetime(time)
        file_time_obj = pd.to_datetime(selected_file['time'])

        if self.sounding_params['requires_lon_conversion'] == True and sounding_lon < 0:
            sounding_lon += 360

        if selected_file['data_format'] == 'grib':
            engine = 'cfgrib'
        else:
            engine = 'netcdf4'

        if self.selected_chunk['model_name'] == "hrrr": #HRRR

            data = xr.open_dataset(selected_file['file_path'], 
                                 engine = engine,
                                 filter_by_keys = {'typeOfLevel': 'isobaricInhPa'},
                                 errors='ignore')

            abslat = np.abs(data.latitude-sounding_lat)
            abslon = np.abs(data.longitude-sounding_lon)
            c = np.maximum(abslon, abslat)
            ([idx_y], [idx_x]) = np.where(c == np.min(c))

            data_pt = data.sel(y=idx_y, x=idx_x)
            selected_point = (data_pt.latitude.data, data_pt.longitude.data)

            p_mb = data_pt.isobaricInhPa.data * units("hPa")
            T_K = data_pt.t.data * units("K")
            T_C = T_K.to(units("degC"))
            Td_K = data_pt.dpt.data * units("K")
            Td_C = Td_K.to(units("degC"))
            u_ms = data_pt.u.data * units("m/s")
            u_kt = u_ms.to(units("knot"))
            v_ms = data_pt.v.data * units("m/s")
            v_kt = v_ms.to(units("knot"))
            z_gpm = data_pt.gh.data * units("gpm")

            sfc_data = xr.open_dataset(selected_file['file_path'], 
                         engine = engine,
                         filter_by_keys = {'typeOfLevel': 'surface', 'stepType': 'instant'},
                         errors='ignore')
            sfc_data_pt = sfc_data.sel(y=idx_y, x=idx_x)

            psfc_Pa = sfc_data_pt.sp.data * units("Pa")
            psfc_mb = psfc_Pa.to(units("hPa"))
            groundlvl_m = sfc_data_pt.orog.data * units("m")

            data_2m = xr.open_dataset(selected_file['file_path'], 
                        engine = engine,
                        filter_by_keys = {'typeOfLevel': 'heightAboveGround', 'level' : 2},
                        errors='ignore')
            data_2m_pt = data_2m.sel(y=idx_y, x=idx_x)

            T2m_K = data_2m_pt.t2m.data * units("K")
            T2m_C = T2m_K.to(units("degC"))
            Td2m_K = data_2m_pt.d2m.data * units("K")
            Td2m_C = Td2m_K.to(units("degC"))

            p2m_mb = psfc_mb - (0.2 * units("hPa"))
            lvl2m_m = groundlvl_m + (2.0 * units("m"))

            data_10m = xr.open_dataset(selected_file['file_path'], 
                     engine = engine,
                     filter_by_keys = {'typeOfLevel': 'heightAboveGround', 'level': 10},
                     errors='ignore')
            data_10m_pt = data_10m.sel(y=idx_y, x=idx_x)

            u10m_ms = data_10m_pt.u10.data * units("m/s")
            u10m_kt = u10m_ms.to(units("knot"))
            v10m_ms = data_10m_pt.v10.data * units("m/s")
            v10m_kt = v10m_ms.to(units("knot"))

            ua_df = pd.DataFrame(data=[p_mb.magnitude, T_C.magnitude, Td_C.magnitude, u_kt.magnitude, v_kt.magnitude, z_gpm.magnitude])
            sfc_df = pd.DataFrame(data=[p2m_mb.magnitude, T2m_C.magnitude, Td2m_C.magnitude, u10m_kt.magnitude, v10m_kt.magnitude, lvl2m_m.magnitude])
            sounding_df = pd.concat([sfc_df.T, ua_df.T],
                                    axis=0,
                                    ignore_index=True)

        elif self.selected_chunk['model_name'] == "gfs":
            data = xr.open_dataset(selected_file['file_path'], 
                     engine = engine,
                     filter_by_keys = {'typeOfLevel': 'isobaricInhPa'},
                     errors='ignore')

            data_pt = data.sel(latitude=sounding_lat, longitude=sounding_lon, method='nearest')
            selected_point = (data_pt.latitude.data, data_pt.longitude.data)

            p_mb = data_pt.isobaricInhPa.data * units("hPa")
            T_K = data_pt.t.data * units("K")
            T_C = T_K.to(units("degC"))
            rh = data_pt.r.data * units("%")
            Td_C = mpcalc.dewpoint_from_relative_humidity(T_C, rh)
            u_ms = data_pt.u.data * units("m/s")
            u_kt = u_ms.to(units("knot"))
            v_ms = data_pt.v.data * units("m/s")
            v_kt = v_ms.to(units("knot"))
            z_gpm = data_pt.gh.data * units("gpm")

            sfc_data = xr.open_dataset(selected_file['file_path'], 
                         engine = engine,
                         filter_by_keys = {'typeOfLevel': 'surface', 'stepType': 'instant'},
                         errors='ignore')
            sfc_data_pt = sfc_data.sel(latitude=sounding_lat, longitude=sounding_lon, method='nearest')

            psfc_Pa = sfc_data_pt.sp.data * units("Pa")
            psfc_mb = psfc_Pa.to(units("hPa"))
            groundlvl_m = sfc_data_pt.orog.data * units("m")

            data_2m = xr.open_dataset(selected_file['file_path'], 
                        engine = engine,
                        filter_by_keys = {'typeOfLevel': 'heightAboveGround', 'level' : 2},
                        errors='ignore')
            data_2m_pt = data_2m.sel(latitude=sounding_lat, longitude=sounding_lon, method='nearest')

            T2m_K = data_2m_pt.t2m.data * units("K")
            T2m_C = T2m_K.to(units("degC"))
            Td2m_K = data_2m_pt.d2m.data * units("K")
            Td2m_C = Td2m_K.to(units("degC"))
            p2m_mb = psfc_mb - (0.2 * units("hPa"))
            lvl2m_m = groundlvl_m + (2.0 * units("m"))

            data_10m = xr.open_dataset(selected_file['file_path'], 
                     engine = engine,
                     filter_by_keys = {'typeOfLevel': 'heightAboveGround', 'level': 10},
                     errors='ignore')
            data_10m_pt = data_10m.sel(latitude=sounding_lat, longitude=sounding_lon, method='nearest')

            u10m_ms = data_10m_pt.u10.data * units("m/s")
            u10m_kt = u10m_ms.to(units("knot"))
            v10m_ms = data_10m_pt.v10.data * units("m/s")
            v10m_kt = v10m_ms.to(units("knot"))

            ua_df = pd.DataFrame(data=[p_mb.magnitude, T_C.magnitude, Td_C.magnitude, u_kt.magnitude, v_kt.magnitude, z_gpm.magnitude])
            sfc_df = pd.DataFrame(data=[p2m_mb.magnitude, T2m_C.magnitude, Td2m_C.magnitude, u10m_kt.magnitude, v10m_kt.magnitude, lvl2m_m.magnitude])
            sounding_df = pd.concat([sfc_df.T, ua_df.T],
                                    axis=0,
                                    ignore_index=True)

        #Remove NaN's for winds
        sounding_df[3].fillna("", inplace=True)
        sounding_df[4].fillna("", inplace=True)

        #Filter out levels below the surface
        sounding_df = sounding_df[sounding_df[0] <= psfc_mb.magnitude]
        sounding_df = sounding_df.round(decimals=2)

        if not sounding_name:
            raob_id = selected_file['file_name']
            file_name = f"RAOBsounding_pt{time_obj.strftime('%Y%m%d_%H%M%SZ')}_{self.selected_chunk['model_name']}{file_time_obj.strftime('%Y%m%d_%H%M%SZ')}.csv"
        else:
            raob_id = sounding_name
            file_name = f"{sounding_name}_RAOBsounding_pt{time_obj.strftime('%Y%m%d_%H%M%SZ')}_{self.selected_chunk['model_name']}{file_time_obj.strftime('%Y%m%d_%H%M%SZ')}.csv"

        raob_header = {0:['RAOB/CSV','DTG','LAT','LON','ELEV','MOISTURE','WIND','GPM','MISSING','RAOB/DATA','PRES'],
                       1:[raob_id,selected_file['time'],selected_point[0],selected_point[1],round(lvl2m_m.magnitude),'TD','kts','MSL',-999,'','TEMP'],
                       2:['','','N','W','m','','U/V','','','','TD'],
                       3:['','','','','','','','','','','UU'],
                       4:['','','','','','','','','','','VV'],
                       5:['','','','','','','','','','','GPM']}

        header_df = pd.DataFrame(data=raob_header)
        raob_df = pd.concat([header_df,sounding_df], axis=0, ignore_index=True)

        output_dir = os.path.join(self.working_dir, 'RAOB Soundings', self.selected_chunk['model_name'])
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        output_path = os.path.join(output_dir, file_name)
        raob_df.to_csv(output_path, index=False, header=False)


def create(storage_dir_path, product):
    return _ProcessorInstantiator(storage_dir_path, product)

if __name__ == "__main__":

    #ipython compatability
    __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"

    # new_proc = create('/Users/rpurciel/Development/NON GH/test', "gfs")

    start_time = datetime(2020, 2, 8, 14, 0)
    end_time = datetime(2020, 2, 8, 16, 0)

    # new_proc.archive_download(start_time, end_time)

    hrrr_proc = create('/Users/rpurciel/Documents/Sims v Honeywell/HRRR Cross Section', "hrrr")

    hrrr_proc.archive_download(start_time, end_time)

    Path = PointPath(from_csv="/Users/rpurciel/Documents/Sims v Honeywell/HRRR Cross Section/path.csv")

    hrrr_proc.sounding('cross-section', Path)

    # path = PointPath(from_csv="/Users/rpurciel/Documents/ContrailHunters Verification/minden.csv")

    # print(path.tags['alt_units'])

















