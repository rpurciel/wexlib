import sys
import glob
import warnings
import datetime
import os
warnings.filterwarnings('ignore')
sys.path.insert(0, '/Users/rpurciel/Development/wexlib/src') #FOR TESTING ONLY!!!

from bs4 import BeautifulSoup, NavigableString
import pandas as pd
import lxml.etree
import lxml.builder

import wexlib.model.era5 as era5
import wexlib.model.hrrr as hrrr
import wexlib.model.gfs as gfs
import wexlib.model.access as access
#import wexlib.model.wrf as wrf
import wexlib.sat.goes as goes
import wexlib.sat.himawari as himawari
from processing_util import calc_time_remaining, calc_total_time

def processor_selector(command_file_path, **kwargs):

    if kwargs.get('verbose') == True:
        verbose = True
        print("INFO: VERBOSE mode turned ON")
        endchar = "\n"
    else:
        verbose = False
        endchar = "\r"

    if kwargs.get('debug') == True:
        debug = True
        verbose = True
        print("INFO: DEBUG mode turned ON")
        endchar = "\n"
    else:
        debug = False
        endchar = "\r"

    if debug:
        print("DEBUG: Kwargs passed:", kwargs)

    with open(command_file_path) as cmd_file:
        cmd = BeautifulSoup(cmd_file, 'xml')
    print("File read successfully!")

    platform = cmd.info.attrs['platform']
    product = cmd.info.attrs['product']

    if platform == "goes16" or platform == "goes17" or platform == "goes18":
        platform_abr = "G1" + str(platform[-1:])

        if product == "download":
            print("Starting GOES AWS downloader...")

            ###INPUT PARSING

            save_dir = cmd.save_dir.text.strip()
            sector = cmd.sector.text.strip()

            start_year = int(cmd.start_time.attrs['year'])
            start_month = int(cmd.start_time.attrs['month'])
            start_day = int(cmd.start_time.attrs['day'])
            start_hour = int(cmd.start_time.attrs['hour'])
            start_minute = int(cmd.start_time.attrs['minute'])

            end_year = int(cmd.end_time.attrs['year'])
            end_month = int(cmd.end_time.attrs['month'])
            end_day = int(cmd.end_time.attrs['day'])
            end_hour = int(cmd.end_time.attrs['hour'])
            end_minute = int(cmd.end_time.attrs['minute'])

            cmd_args = cmd.cmd_args.attrs

            if verbose:
                print(f'''DATA: Input data info below\nDATA: Platform: {platform}\nDATA: Product: {product}'''
                      f'''\nDATA: Start time: {str(start_year).zfill(4)}-{str(start_month).zfill(2)}-{str(start_day).zfill(2)} {str(start_hour).zfill(2)}:00:00 UTC'''
                      f'''\nDATA: End time:   {str(end_year).zfill(4)}-{str(end_month).zfill(2)}-{str(end_day).zfill(2)} {str(end_hour).zfill(2)}:00:00 UTC'''
                      f'''\nDATA: Save directory: {save_dir}\nDATA: Sector: {sector}\nDATA: Args to pass: {cmd_args}''')

            start_date = datetime.datetime(start_year, start_month, start_day, start_hour)
            end_date = datetime.datetime(end_year, end_month, end_day, end_hour)

            ###PROCESSING

            time_period = pd.period_range(start=start_date, end=end_date, freq="1H")

            if debug:
                print("PROCESSING: Time range: ")
                print(time_period)

            bucket_times = []
            tot_buckets = len(time_period)
            num_buckets = 1
            tot_files_all_buckets = 0
            total_processing_time = 0

            for time in time_period:
                year = time.year
                month = time.month
                doy = time.dayofyear
                day = time.day
                hour = time.hour
                minute = time.minute
                if verbose:
                    print(f"PROCESSING: Selected time {str(year).zfill(4)}-{str(month).zfill(2)}-{str(day).zfill(2)} {str(hour).zfill(2)}:00:00 UTC")

                print(f'{num_buckets}/{tot_buckets} AWS buckets indexed.')

                status, files = goes.aws_read_bucket(year, doy, hour, platform, sector, **cmd_args)

                num_buckets += status
                tot_files = len(files)
                num_files = 0
                tot_time = 0
                processing_time = 0
                plot_times = []

                if debug:
                	print("DEBUG: File URLs")
                	print(files)

                for file in files:

                    status, time, path = goes.download_single_file_aws(save_dir, file)

                    num_files += status
                    tot_time += time
                    plot_times += [time]
                    time_remaining = calc_time_remaining(tot_files, num_files, plot_times)

                    print(f'{num_files}/{tot_files} files downloaded from bucket. Est. time remaining: {time_remaining}', end=endchar)

                bucket_time_str = calc_total_time(tot_time)
                tot_files_all_buckets += num_files
                bucket_times += [tot_time]
                processing_time += tot_time
                processing_time_est = calc_time_remaining(tot_buckets, num_buckets, bucket_times)

                print(f'Finished downloading {num_files} files from bucket. Total time: {bucket_time_str}         ')
                print(f'Estimated time remaining: {processing_time_est}\n')

            ###POSTPROCESSING

            processing_time_str = calc_total_time(processing_time)
            tot_files_all_buckets += num_files
            print(f'Finished downloading {tot_files_all_buckets} files from {num_buckets} buckets. Total time: {processing_time_str}')

            # xml = lxml.builder.ElementMaker()
            # output_cmd_template = xml.command(xml.info(platform=platform), 
            #                                   xml.params(
            #                                              xml.start_time(year=start_year, month=start_month, 
            #                                                             day=start_day, hour=start_hour, 
            #                                                             minute=start_minute),
            #                                              xml.end_time(year=end_year, month=end_month, 
            #                                                           day=end_day, hour=end_hour, 
            #                                                           minute=end_minute),
            #                                              xml.data_dir(save_dir)
            #                                              )
            #                                   )


            # output_cmd_name = platform_abr + "_" + datetime.now().strftime("%Y%m%d_%h%M%S") + "_data_cmdbase.xml"
            # output_cmd_template.write(os.path.join(os.path.dirname(save_dir), ''))

        if product == "plot_single_band":
            print("Starting Himawari Single Band plotter...")

            ###INPUT PARSING

            data_dir = cmd.data_dir.text.strip()
            save_dir = cmd.save_dir.text.strip()
            band_num = int(cmd.band_num.text.strip())
            pallete = cmd.pallete.text.strip()

            start_year = int(cmd.start_time.attrs['year'])
            start_month = int(cmd.start_time.attrs['month'])
            start_day = int(cmd.start_time.attrs['day'])
            start_hour = int(cmd.start_time.attrs['hour'])
            start_minute = int(cmd.start_time.attrs['minute'])

            end_year = int(cmd.end_time.attrs['year'])
            end_month = int(cmd.end_time.attrs['month'])
            end_day = int(cmd.end_time.attrs['day'])
            end_hour = int(cmd.end_time.attrs['hour'])
            end_minute = int(cmd.end_time.attrs['minute'])

            points_list = []
            for point in cmd.points.children:
                if not isinstance(point, NavigableString):
                    lat = float(point.attrs['lat'])
                    lon = float(point.attrs['lon'])
                    label = point.attrs['label']
                    points_list.append((lat, lon, label))

            bbox = [0, 0, 0, 0] #NESW
            for bbox_param in cmd.bbox.attrs:
                if bbox_param == 'north':
                    bbox[0] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'east':
                    bbox[1] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'south':
                    bbox[2] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'west':
                    bbox[3] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'center_lat' or bbox_param == 'center_lon' or bbox_param == 'radius':
                    center_lat = float(cmd.bbox.attrs['center_lat'])
                    center_lon = float(cmd.bbox.attrs['center_lon'])
                    radius = float(cmd.bbox.attrs['radius'])
                    bbox = [center_lat + radius, center_lon + radius, center_lat - radius, center_lon - radius]

            cmd_args = cmd.cmd_args.attrs

            if verbose:
                print(f'''DATA: Input data info below\nDATA: Platform: {platform}\nDATA: Product: {product}'''
                      f'''\nDATA: Start time: {str(start_year).zfill(4)}-{str(start_month).zfill(2)}-{str(start_day).zfill(2)} {str(start_hour).zfill(2)}:{str(start_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: End time:   {str(end_year).zfill(4)}-{str(end_month).zfill(2)}-{str(end_day).zfill(2)} {str(end_hour).zfill(2)}:{str(end_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: Data directory: {data_dir}\nDATA: Save directory: {save_dir}\nDATA: Selected Pallete (if any): {pallete}'''
                      f'''\nDATA: Bounding box: {bbox}\nDATA: Args to pass: {cmd_args}\nDATA: Points to plot: {len(points_list)}''')
            if debug:
                print(f'DEBUG: Point data: \n{points_list}')

            start_date = datetime.datetime(start_year, start_month, start_day, start_hour, start_minute)
            end_date = datetime.datetime(end_year, end_month, end_day, end_hour, end_minute)

            ###PROCESSING

            time_period = pd.period_range(start=start_date, end=end_date, freq="10min")

            if debug:
                print("PROCESSING: Time range: ")
                print(time_period)

            num_files = 0
            tot_time = 0
            plot_times = []
            tot_files = len(time_period)

            for time in time_period:
                year = time.year
                month = time.month
                day = time.day
                hour = time.hour
                minute = time.minute
                if verbose:
                    print(f"PROCESSING: Selected time {str(year).zfill(4)}-{str(month).zfill(2)}-{str(day).zfill(2)} {str(hour).zfill(2)}:{str(minute).zfill(2)}:00 UTC")

                file_pattern = f"HS_{platform_abr}_{year}{str(month).zfill(2)}{str(day).zfill(2)}_{str(hour).zfill(2)}{str(minute).zfill(2)}*.bz2"

                input_files = sorted(glob.glob(data_dir + "/" + file_pattern))

                if not input_files:
                    if verbose:
                        print("PROCESSING: No files found for time. Skipping...")
                        continue
                else:
                    if verbose:
                        print(f"PROCESSING: Files found for time. Starting processor...")
                    if pallete:
                        status, time, path = himawari.plot_single_band(input_files, save_dir, 
                                                                       band_num, points_list, 
                                                                       bbox, pallete, **cmd_args)
                    else:
                        status, time, path = himawari.plot_single_band(input_files, save_dir, 
                                                                       band_num, points_list, 
                                                                       bbox, **cmd_args)
                    num_files += status
                    tot_time += time
                    plot_times += [time]

                time_remaining = calc_time_remaining(tot_files, num_files, plot_times)
                print(f'{num_files}/{tot_files} plots created. Est. time remaining: {time_remaining}', end=endchar)

            ###POSTPROCESSING

            tot_time_str = calc_total_time(tot_time)
            print(f'Finished creating {num_files} plots. Total time: {tot_time_str}         ')

        if product == "plot_composite":
            print("Starting GOES Composite plotter...")

            ###INPUT PARSING

            data_dir = cmd.data_dir.text.strip()
            save_dir = cmd.save_dir.text.strip()
            sat_product = cmd.product.text.strip()

            start_year = int(cmd.start_time.attrs['year'])
            start_month = int(cmd.start_time.attrs['month'])
            start_day = int(cmd.start_time.attrs['day'])
            start_hour = int(cmd.start_time.attrs['hour'])
            start_minute = int(cmd.start_time.attrs['minute'])

            end_year = int(cmd.end_time.attrs['year'])
            end_month = int(cmd.end_time.attrs['month'])
            end_day = int(cmd.end_time.attrs['day'])
            end_hour = int(cmd.end_time.attrs['hour'])
            end_minute = int(cmd.end_time.attrs['minute'])

            points_list = []
            for point in cmd.points.children:
                if not isinstance(point, NavigableString):
                    lat = float(point.attrs['lat'])
                    lon = float(point.attrs['lon'])
                    label = point.attrs['label']
                    points_list.append((lat, lon, label))

            bbox = [0, 0, 0, 0] #NESW
            for bbox_param in cmd.bbox.attrs:
                if bbox_param == 'north':
                    bbox[0] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'east':
                    bbox[1] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'south':
                    bbox[2] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'west':
                    bbox[3] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'center_lat' or bbox_param == 'center_lon' or bbox_param == 'radius':
                    center_lat = float(cmd.bbox.attrs['center_lat'])
                    center_lon = float(cmd.bbox.attrs['center_lon'])
                    radius = float(cmd.bbox.attrs['radius'])
                    bbox = [center_lat + radius, center_lon + radius, center_lat - radius, center_lon - radius]

            cmd_args = cmd.cmd_args.attrs

            if verbose:
                print(f'''DATA: Input data info below\nDATA: Platform: {platform}\nDATA: Product: {product}'''
                      f'''\nDATA: Start time: {str(start_year).zfill(4)}-{str(start_month).zfill(2)}-{str(start_day).zfill(2)} {str(start_hour).zfill(2)}:{str(start_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: End time:   {str(end_year).zfill(4)}-{str(end_month).zfill(2)}-{str(end_day).zfill(2)} {str(end_hour).zfill(2)}:{str(end_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: Data directory: {data_dir}\nDATA: Save directory: {save_dir}\nDATA: Selected Product: {sat_product}'''
                      f'''\nDATA: Bounding box: {bbox}\nDATA: Args to pass: {cmd_args}\nDATA: Points to plot: {len(points_list)}''')
            if debug:
                print(f'DEBUG: Point data: \n{points_list}')

            start_date = datetime.datetime(start_year, start_month, start_day, start_hour, start_minute)
            end_date = datetime.datetime(end_year, end_month, end_day, end_hour, end_minute)

            ###PROCESSING

            time_period = pd.period_range(start=start_date, end=end_date, freq="1sec")

            if debug:
                print("PROCESSING: Time range: ")
                print(time_period)

            num_files = 0
            tot_time = 0
            plot_times = []
            tot_files = len(time_period)

            for time in time_period:
                year = time.year
                month = time.month
                day = time.day
                hour = time.hour
                minute = time.minute
                if verbose:
                    print(f"PROCESSING: Selected time {str(year).zfill(4)}-{str(month).zfill(2)}-{str(day).zfill(2)} {str(hour).zfill(2)}:{str(minute).zfill(2)}:00 UTC")

                file_pattern = f"HS_{platform_abr}_{year}{str(month).zfill(2)}{str(day).zfill(2)}_{str(hour).zfill(2)}{str(minute).zfill(2)}*.bz2"

                input_files = sorted(glob.glob(data_dir + "/" + file_pattern))

                if not input_files:
                    if verbose:
                        print("PROCESSING: No files found for time. Skipping...")
                        continue
                else:
                    if verbose:
                        print(f"PROCESSING: Files found for time. Starting processor...")

                    status, time, path = goes.plot_composite(input_files, save_dir, 
                                                                   sat_product, points_list, 
                                                                   bbox, **cmd_args)
                num_files += status
                tot_time += time
                plot_times += [time]

                time_remaining = calc_time_remaining(tot_files, num_files, plot_times)
                print(f'{num_files}/{tot_files} plots created. Est. time remaining: {time_remaining}', end=endchar)

            ###POSTPROCESSING

            tot_time_str = calc_total_time(tot_time)
            print(f'Finished creating {num_files} plots. Total time: {tot_time_str}         ')

    if platform == "himawari9" or platform == "himawari8":
        platform_abr = "H" + str(platform[-1:]).zfill(2)

        if product == "download":
            print("Starting Himawari AWS downloader...")

            ###INPUT PARSING

            save_dir = cmd.save_dir.text.strip()
            sector = cmd.sector.text.strip()

            start_year = int(cmd.start_time.attrs['year'])
            start_month = int(cmd.start_time.attrs['month'])
            start_day = int(cmd.start_time.attrs['day'])
            start_hour = int(cmd.start_time.attrs['hour'])
            start_minute = int(cmd.start_time.attrs['minute'])

            end_year = int(cmd.end_time.attrs['year'])
            end_month = int(cmd.end_time.attrs['month'])
            end_day = int(cmd.end_time.attrs['day'])
            end_hour = int(cmd.end_time.attrs['hour'])
            end_minute = int(cmd.end_time.attrs['minute'])

            cmd_args = cmd.cmd_args.attrs

            if verbose:
                print(f'''DATA: Input data info below\nDATA: Platform: {platform}\nDATA: Product: {product}'''
                      f'''\nDATA: Start time: {str(start_year).zfill(4)}-{str(start_month).zfill(2)}-{str(start_day).zfill(2)} {str(start_hour).zfill(2)}:{str(start_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: End time:   {str(end_year).zfill(4)}-{str(end_month).zfill(2)}-{str(end_day).zfill(2)} {str(end_hour).zfill(2)}:{str(end_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: Save directory: {save_dir}\nDATA: Sector: {sector}\nDATA: Args to pass: {cmd_args}''')

            start_date = datetime.datetime(start_year, start_month, start_day, start_hour, start_minute)
            end_date = datetime.datetime(end_year, end_month, end_day, end_hour, end_minute)

            ###PROCESSING

            time_period = pd.period_range(start=start_date, end=end_date, freq="10min")

            if debug:
                print("PROCESSING: Time range: ")
                print(time_period)

            bucket_times = []
            tot_buckets = len(time_period)
            num_buckets = 1
            tot_files_all_buckets = 0
            total_processing_time = 0

            for time in time_period:
                year = time.year
                month = time.month
                day = time.day
                hour = time.hour
                minute = time.minute
                if verbose:
                    print(f"PROCESSING: Selected time {str(year).zfill(4)}-{str(month).zfill(2)}-{str(day).zfill(2)} {str(hour).zfill(2)}:{str(minute).zfill(2)}:00 UTC")

                print(f'{num_buckets}/{tot_buckets} AWS buckets indexed.')

                status, files = himawari.aws_read_bucket(year, month, day, hour, minute, 
                                                         platform, sector, **cmd_args)

                num_buckets += status
                tot_files = len(files)
                num_files = 0
                tot_time = 0
                plot_times = []

                if debug:
                	print("DEBUG: File URLs")
                	print(files)

                for file in files:

                    status, time, path = himawari.download_single_file_aws(save_dir, file)

                    num_files += status
                    tot_time += time
                    plot_times += [time]
                    time_remaining = calc_time_remaining(tot_files, num_files, plot_times)

                    print(f'{num_files}/{tot_files} files downloaded from bucket. Est. time remaining: {time_remaining}', end=endchar)

                bucket_time_str = calc_total_time(tot_time)
                tot_files_all_buckets += num_files
                bucket_times += [tot_time]
                processing_time += tot_time
                processing_time_est = calc_time_remaining(tot_buckets, num_buckets, bucket_times)

                print(f'Finished downloading {num_files} files from bucket. Total time: {bucket_time_str}         ')
                print(f'Estimated time remaining: {processing_time_est}\n')

            ###POSTPROCESSING

            processing_time_str = calc_total_time(processing_time)
            tot_files_all_buckets += num_files
            print(f'Finished downloading {tot_files_all_buckets} files from {num_buckets} buckets. Total time: {processing_time_str}')

            # xml = lxml.builder.ElementMaker()
            # output_cmd_template = xml.command(xml.info(platform=platform), 
            #                                   xml.params(
            #                                              xml.start_time(year=start_year, month=start_month, 
            #                                                             day=start_day, hour=start_hour, 
            #                                                             minute=start_minute),
            #                                              xml.end_time(year=end_year, month=end_month, 
            #                                                           day=end_day, hour=end_hour, 
            #                                                           minute=end_minute),
            #                                              xml.data_dir(save_dir)
            #                                              )
            #                                   )


            # output_cmd_name = platform_abr + "_" + datetime.now().strftime("%Y%m%d_%h%M%S") + "_data_cmdbase.xml"
            # output_cmd_template.write(os.path.join(os.path.dirname(save_dir), ''))

        if product == "plot_single_band":
            print("Starting Himawari Single Band plotter...")

            ###INPUT PARSING

            data_dir = cmd.data_dir.text.strip()
            save_dir = cmd.save_dir.text.strip()
            band_num = int(cmd.band_num.text.strip())
            pallete = cmd.pallete.text.strip()

            start_year = int(cmd.start_time.attrs['year'])
            start_month = int(cmd.start_time.attrs['month'])
            start_day = int(cmd.start_time.attrs['day'])
            start_hour = int(cmd.start_time.attrs['hour'])
            start_minute = int(cmd.start_time.attrs['minute'])

            end_year = int(cmd.end_time.attrs['year'])
            end_month = int(cmd.end_time.attrs['month'])
            end_day = int(cmd.end_time.attrs['day'])
            end_hour = int(cmd.end_time.attrs['hour'])
            end_minute = int(cmd.end_time.attrs['minute'])

            points_list = []
            for point in cmd.points.children:
                if not isinstance(point, NavigableString):
                    lat = float(point.attrs['lat'])
                    lon = float(point.attrs['lon'])
                    label = point.attrs['label']
                    points_list.append((lat, lon, label))

            bbox = [0, 0, 0, 0] #NESW
            for bbox_param in cmd.bbox.attrs:
                if bbox_param == 'north':
                    bbox[0] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'east':
                    bbox[1] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'south':
                    bbox[2] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'west':
                    bbox[3] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'center_lat' or bbox_param == 'center_lon' or bbox_param == 'radius':
                    center_lat = float(cmd.bbox.attrs['center_lat'])
                    center_lon = float(cmd.bbox.attrs['center_lon'])
                    radius = float(cmd.bbox.attrs['radius'])
                    bbox = [center_lat + radius, center_lon + radius, center_lat - radius, center_lon - radius]

            cmd_args = cmd.cmd_args.attrs

            if verbose:
                print(f'''DATA: Input data info below\nDATA: Platform: {platform}\nDATA: Product: {product}'''
                      f'''\nDATA: Start time: {str(start_year).zfill(4)}-{str(start_month).zfill(2)}-{str(start_day).zfill(2)} {str(start_hour).zfill(2)}:{str(start_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: End time:   {str(end_year).zfill(4)}-{str(end_month).zfill(2)}-{str(end_day).zfill(2)} {str(end_hour).zfill(2)}:{str(end_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: Data directory: {data_dir}\nDATA: Save directory: {save_dir}\nDATA: Selected Pallete (if any): {pallete}'''
                      f'''\nDATA: Bounding box: {bbox}\nDATA: Args to pass: {cmd_args}\nDATA: Points to plot: {len(points_list)}''')
            if debug:
                print(f'DEBUG: Point data: \n{points_list}')

            start_date = datetime.datetime(start_year, start_month, start_day, start_hour, start_minute)
            end_date = datetime.datetime(end_year, end_month, end_day, end_hour, end_minute)

            ###PROCESSING

            time_period = pd.period_range(start=start_date, end=end_date, freq="10min")

            if debug:
                print("PROCESSING: Time range: ")
                print(time_period)

            num_files = 0
            tot_time = 0
            plot_times = []
            tot_files = len(time_period)

            for time in time_period:
                year = time.year
                month = time.month
                day = time.day
                hour = time.hour
                minute = time.minute
                if verbose:
                    print(f"PROCESSING: Selected time {str(year).zfill(4)}-{str(month).zfill(2)}-{str(day).zfill(2)} {str(hour).zfill(2)}:{str(minute).zfill(2)}:00 UTC")

                file_pattern = f"HS_{platform_abr}_{year}{str(month).zfill(2)}{str(day).zfill(2)}_{str(hour).zfill(2)}{str(minute).zfill(2)}*.bz2"

                input_files = sorted(glob.glob(data_dir + "/" + file_pattern))

                if not input_files:
                    if verbose:
                        print("PROCESSING: No files found for time. Skipping...")
                        continue
                else:
                    if verbose:
                        print(f"PROCESSING: Files found for time. Starting processor...")
                    if pallete:
                        status, time, path = himawari.plot_single_band(input_files, save_dir, 
                                                                       band_num, points_list, 
                                                                       bbox, pallete, **cmd_args)
                    else:
                        status, time, path = himawari.plot_single_band(input_files, save_dir, 
                                                                       band_num, points_list, 
                                                                       bbox, **cmd_args)
                    num_files += status
                    tot_time += time
                    plot_times += [time]

                time_remaining = calc_time_remaining(tot_files, num_files, plot_times)
                print(f'{num_files}/{tot_files} plots created. Est. time remaining: {time_remaining}', end=endchar)

            ###POSTPROCESSING

            tot_time_str = calc_total_time(tot_time)
            print(f'Finished creating {num_files} plots. Total time: {tot_time_str}         ')

        if product == "plot_composite":
            print("Starting Himawari Composite plotter...")

            ###INPUT PARSING

            data_dir = cmd.data_dir.text.strip()
            save_dir = cmd.save_dir.text.strip()
            sat_product = cmd.product.text.strip()

            start_year = int(cmd.start_time.attrs['year'])
            start_month = int(cmd.start_time.attrs['month'])
            start_day = int(cmd.start_time.attrs['day'])
            start_hour = int(cmd.start_time.attrs['hour'])
            start_minute = int(cmd.start_time.attrs['minute'])

            end_year = int(cmd.end_time.attrs['year'])
            end_month = int(cmd.end_time.attrs['month'])
            end_day = int(cmd.end_time.attrs['day'])
            end_hour = int(cmd.end_time.attrs['hour'])
            end_minute = int(cmd.end_time.attrs['minute'])

            points_list = []
            for point in cmd.points.children:
                if not isinstance(point, NavigableString):
                    lat = float(point.attrs['lat'])
                    lon = float(point.attrs['lon'])
                    label = point.attrs['label']
                    points_list.append((lat, lon, label))

            bbox = [0, 0, 0, 0] #NESW
            for bbox_param in cmd.bbox.attrs:
                if bbox_param == 'north':
                    bbox[0] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'east':
                    bbox[1] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'south':
                    bbox[2] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'west':
                    bbox[3] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'center_lat' or bbox_param == 'center_lon' or bbox_param == 'radius':
                    center_lat = float(cmd.bbox.attrs['center_lat'])
                    center_lon = float(cmd.bbox.attrs['center_lon'])
                    radius = float(cmd.bbox.attrs['radius'])
                    bbox = [center_lat + radius, center_lon + radius, center_lat - radius, center_lon - radius]

            cmd_args = cmd.cmd_args.attrs

            if verbose:
                print(f'''DATA: Input data info below\nDATA: Platform: {platform}\nDATA: Product: {product}'''
                      f'''\nDATA: Start time: {str(start_year).zfill(4)}-{str(start_month).zfill(2)}-{str(start_day).zfill(2)} {str(start_hour).zfill(2)}:{str(start_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: End time:   {str(end_year).zfill(4)}-{str(end_month).zfill(2)}-{str(end_day).zfill(2)} {str(end_hour).zfill(2)}:{str(end_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: Data directory: {data_dir}\nDATA: Save directory: {save_dir}\nDATA: Selected Product: {sat_product}'''
                      f'''\nDATA: Bounding box: {bbox}\nDATA: Args to pass: {cmd_args}\nDATA: Points to plot: {len(points_list)}''')
            if debug:
                print(f'DEBUG: Point data: \n{points_list}')

            start_date = datetime.datetime(start_year, start_month, start_day, start_hour, start_minute)
            end_date = datetime.datetime(end_year, end_month, end_day, end_hour, end_minute)

            ###PROCESSING

            time_period = pd.period_range(start=start_date, end=end_date, freq="10min")

            if debug:
                print("PROCESSING: Time range: ")
                print(time_period)

            num_files = 0
            tot_time = 0
            plot_times = []
            tot_files = len(time_period)

            for time in time_period:
                year = time.year
                month = time.month
                day = time.day
                hour = time.hour
                minute = time.minute
                if verbose:
                    print(f"PROCESSING: Selected time {str(year).zfill(4)}-{str(month).zfill(2)}-{str(day).zfill(2)} {str(hour).zfill(2)}:{str(minute).zfill(2)}:00 UTC")

                file_pattern = f"HS_{platform_abr}_{year}{str(month).zfill(2)}{str(day).zfill(2)}_{str(hour).zfill(2)}{str(minute).zfill(2)}*.bz2"

                input_files = sorted(glob.glob(data_dir + "/" + file_pattern))

                if not input_files:
                    if verbose:
                        print("PROCESSING: No files found for time. Skipping...")
                        continue
                else:
                    if verbose:
                        print(f"PROCESSING: Files found for time. Starting processor...")

                    status, time, path = himawari.plot_composite(input_files, save_dir, 
                                                                   sat_product, points_list, 
                                                                   bbox, **cmd_args)
                num_files += status
                tot_time += time
                plot_times += [time]

                time_remaining = calc_time_remaining(tot_files, num_files, plot_times)
                print(f'{num_files}/{tot_files} plots created. Est. time remaining: {time_remaining}', end=endchar)

            ###POSTPROCESSING

            tot_time_str = calc_total_time(tot_time)
            print(f'Finished creating {num_files} plots. Total time: {tot_time_str}         ')

    if platform == "era5":
        platform_abr = "ERA5"

        if product == "download":
            print("Starting ERA5 downloader...")

            ###INPUT PARSING

            save_dir = cmd.save_dir.text.strip()

            start_year = int(cmd.start_time.attrs['year'])
            start_month = int(cmd.start_time.attrs['month'])
            start_day = int(cmd.start_time.attrs['day'])
            start_hour = int(cmd.start_time.attrs['hour'])
            start_minute = int(cmd.start_time.attrs['minute'])

            end_year = int(cmd.end_time.attrs['year'])
            end_month = int(cmd.end_time.attrs['month'])
            end_day = int(cmd.end_time.attrs['day'])
            end_hour = int(cmd.end_time.attrs['hour'])
            end_minute = int(cmd.end_time.attrs['minute'])

            bbox = [0] * 4
            for bbox_param in cmd.bbox.attrs:
                if bbox_param == 'north':
                    bbox[0] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'east':
                    bbox[1] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'south':
                    bbox[2] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'west':
                    bbox[3] = float(cmd.bbox.attrs[bbox_param])
                if bbox_param == 'center_lat' or bbox_param == 'center_lon' or bbox_param == 'radius':
                    center_lat = float(cmd.bbox.attrs['center_lat'])
                    center_lon = float(cmd.bbox.attrs['center_lon'])
                    radius = float(cmd.bbox.attrs['radius'])
                    bbox = [center_lat + radius, center_lon + radius, center_lat - radius, center_lon - radius]
            if bbox == [0] * 4:
            	bbox = None

            cmd_args = cmd.cmd_args.attrs

            if verbose:
                print(f'''DATA: Input data info below\nDATA: Platform: {platform}\nDATA: Product: {product}'''
                      f'''\nDATA: Start time: {str(start_year).zfill(4)}-{str(start_month).zfill(2)}-{str(start_day).zfill(2)} {str(start_hour).zfill(2)}:{str(start_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: End time:   {str(end_year).zfill(4)}-{str(end_month).zfill(2)}-{str(end_day).zfill(2)} {str(end_hour).zfill(2)}:{str(end_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: Save directory: {save_dir}\nDATA: Bounding box: {bbox}\nDATA: Args to pass: {cmd_args}''')

            start_date = datetime.datetime(start_year, start_month, start_day, start_hour, start_minute)
            end_date = datetime.datetime(end_year, end_month, end_day, end_hour, end_minute)

            ###PROCESSING

            time_period = pd.period_range(start=start_date, end=end_date, freq="1H")

            if debug:
                print("PROCESSING: Time range: ")
                print(time_period)

            num_files = 0
            tot_time = 0
            plot_times = []
            tot_files = len(time_period)

            for time in time_period:
                year = time.year
                month = time.month
                day = time.day
                hour = time.hour
                minute = time.minute

                if verbose:
                    print(f"PROCESSING: Downloading time {str(year).zfill(4)}-{str(month).zfill(2)}-{str(day).zfill(2)} {str(hour).zfill(2)}:{str(minute).zfill(2)}:00 UTC")

                if bbox:
                	status, time, ua_path, sfc_path = era5.download(save_dir, year, month, day, hour, bbox, **cmd_args)
               	else:
               		status, time, ua_path, sfc_path = era5.download(save_dir, year, month, day, hour, **cmd_args)

                num_files += status
                tot_time += time
                plot_times += [time]

                time_remaining = calc_time_remaining(tot_files, num_files, plot_times)
                print(f'{num_files}/{tot_files} files downloaded. Est. time remaining: {time_remaining}', end=endchar)

            ###POSTPROCESSING

            tot_time_str = calc_total_time(tot_time)
            print(f'Finished downloading {num_files} files. Total time: {tot_time_str}         ')

            # xml = lxml.builder.ElementMaker()
            # output_cmd_template = xml.command(xml.info(platform=platform), 
            #                                   xml.params(
            #                                              xml.start_time(year=start_year, month=start_month, 
            #                                                             day=start_day, hour=start_hour, 
            #                                                             minute=start_minute),
            #                                              xml.end_time(year=end_year, month=end_month, 
            #                                                           day=end_day, hour=end_hour, 
            #                                                           minute=end_minute),
            #                                              xml.data_dir(save_dir)
            #                                              )
            #                                   )

            # output_cmd_name = platform_abr + "_" + datetime.now().strftime("%Y%m%d_%h%M%S") + "_data_cmdbase.xml"
            # output_cmd_template.write(os.path.join(os.path.dirname(save_dir), ''))

        if product == "model_sounding_raobcsv":
            print("Starting ERA5 Model Sounding (for RAOB) generator...")

            ###INPUT PARSING

            data_dir = cmd.data_dir.text.strip()
            save_dir = cmd.save_dir.text.strip()

            start_year = int(cmd.start_time.attrs['year'])
            start_month = int(cmd.start_time.attrs['month'])
            start_day = int(cmd.start_time.attrs['day'])
            start_hour = int(cmd.start_time.attrs['hour'])
            start_minute = int(cmd.start_time.attrs['minute'])

            end_year = int(cmd.end_time.attrs['year'])
            end_month = int(cmd.end_time.attrs['month'])
            end_day = int(cmd.end_time.attrs['day'])
            end_hour = int(cmd.end_time.attrs['hour'])
            end_minute = int(cmd.end_time.attrs['minute'])

            points_list = []
            for point in cmd.points.children:
                if not isinstance(point, NavigableString):
                    lat = float(point.attrs['lat'])
                    lon = float(point.attrs['lon'])
                    year = int(point.attrs['year'])
                    month = int(point.attrs['month'])
                    day = int(point.attrs['day'])
                    hour = int(point.attrs['hour'])
                    minute = int(point.attrs['minute'])
                    second = int(point.attrs['second'])
                    alt_ft = float(point.attrs['alt_ft'])

                    timestamp = pd.Timestamp(year=year, month=month, day=day, hour=hour, minute=minute, second=second)
                    points_list.append((lat, lon, alt_ft, timestamp))

            cmd_args = cmd.cmd_args.attrs

            if verbose:
                print(f'''DATA: Input data info below\nDATA: Platform: {platform}\nDATA: Product: {product}'''
                      f'''\nDATA: Start time: {str(start_year).zfill(4)}-{str(start_month).zfill(2)}-{str(start_day).zfill(2)} {str(start_hour).zfill(2)}:{str(start_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: End time:   {str(end_year).zfill(4)}-{str(end_month).zfill(2)}-{str(end_day).zfill(2)} {str(end_hour).zfill(2)}:{str(end_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: Data directory: {data_dir}\nDATA: Save directory: {save_dir}'''
                      f'''\nDATA: Args to pass: {cmd_args}\nDATA: Points to plot: {len(points_list)}''')
            if debug:
                print(f'DEBUG: Point data: \n{points_list}')

            start_date = datetime.datetime(start_year, start_month, start_day, start_hour, start_minute)
            end_date = datetime.datetime(end_year, end_month, end_day, end_hour, end_minute)

            ###PROCESSING

            data_period = pd.period_range(start=start_date, end=end_date, freq="1H")

            num_files = 0
            tot_time = 0
            plot_times = []
            tot_files = len(points_list)
            prev_points = []

            for point in points_list:
                point_lat = point[0]
                point_lon = point[1]
                point_alt = point[2]
                point_time = point[3]

                if verbose:
                    print(f"PROCESSING: Selected point ({point_lat}, {point_lon})")

                lowest_sec = 999999999
                sel_time = point_time

                for data_time in data_period:
                    delta_this_time = data_time.to_timestamp() - point_time
                    sec_diff = abs(delta_this_time.total_seconds())
                    if sec_diff < lowest_sec:
                        lowest_sec = sec_diff
                        sel_time = data_time.to_timestamp()

                sel_year = sel_time.year
                sel_month = sel_time.month
                sel_day = sel_time.day
                sel_hour = sel_time.hour

                if debug:
                    print(f"DEBUG: Selected time: {sel_time}")
                    print(f"DEBUG: Point time: {point_time}")
                    print(f"DEBUG: Seconds difference: {lowest_sec}")

                ua_file_name = f"{sel_year}_{str(sel_month).zfill(2)}_{str(sel_day).zfill(2)}_{str(sel_hour).zfill(2)}00_UA_ERA5.grib"
                sfc_file_name = f"{sel_year}_{str(sel_month).zfill(2)}_{str(sel_day).zfill(2)}_{str(sel_hour).zfill(2)}00_SFC_ERA5.grib"

                ua_path = os.path.join(data_dir, ua_file_name)
                sfc_path = os.path.join(data_dir, sfc_file_name)

                title = point_time.strftime("FLT%Y%m%d_%H%M%S")

                if not (os.path.isfile(ua_path) or os.path.isfile(sfc_path)):
                    if verbose:
                        print("PROCESSING: No files found for time. Skipping...")
                    continue
                else:
                    if verbose:
                        print(f"PROCESSING: Files found for time. Starting processor...")
                    status, time, model_point = era5.model_sounding_raobcsv(ua_path, sfc_path, save_dir, 
                                                                     			point_lat, point_lon, prev_points,
                                                                     			sounding_title=title, **cmd_args)

                    num_files += status
                    tot_time += time
                    plot_times += [time]
                    prev_points += [model_point]

                time_remaining = calc_time_remaining(tot_files, num_files, plot_times)
                print(f'{num_files}/{tot_files} soundings created. Est. time remaining: {time_remaining}', end=endchar)

            ###POSTPROCESSING

            tot_time_str = calc_total_time(tot_time)
            print(f'Finished creating {num_files} soundings. Total time: {tot_time_str}         ')

    if platform == "access":
        platform_abr = "ACCESS (AU)"

        if product == "model_sounding_raobcsv":

            print("Starting ACCESS (AU) Model Sounding (for RAOB) generator...")

            ###INPUT PARSING

            data_dir = cmd.data_dir.text.strip()
            save_dir = cmd.save_dir.text.strip()

            start_year = int(cmd.start_time.attrs['year'])
            start_month = int(cmd.start_time.attrs['month'])
            start_day = int(cmd.start_time.attrs['day'])
            start_hour = int(cmd.start_time.attrs['hour'])
            start_minute = int(cmd.start_time.attrs['minute'])

            end_year = int(cmd.end_time.attrs['year'])
            end_month = int(cmd.end_time.attrs['month'])
            end_day = int(cmd.end_time.attrs['day'])
            end_hour = int(cmd.end_time.attrs['hour'])
            end_minute = int(cmd.end_time.attrs['minute'])

            points_list = []
            for point in cmd.points.children:
                if not isinstance(point, NavigableString):
                    lat = float(point.attrs['lat'])
                    lon = float(point.attrs['lon'])
                    year = int(point.attrs['year'])
                    month = int(point.attrs['month'])
                    day = int(point.attrs['day'])
                    hour = int(point.attrs['hour'])
                    minute = int(point.attrs['minute'])
                    second = int(point.attrs['second'])
                    alt_ft = float(point.attrs['alt_ft'])

                    timestamp = pd.Timestamp(year=year, month=month, day=day, hour=hour, minute=minute, second=second)
                    points_list.append((lat, lon, alt_ft, timestamp))

            cmd_args = cmd.cmd_args.attrs

            if verbose:
                print(f'''DATA: Input data info below\nDATA: Platform: {platform}\nDATA: Product: {product}'''
                      f'''\nDATA: Start time: {str(start_year).zfill(4)}-{str(start_month).zfill(2)}-{str(start_day).zfill(2)} {str(start_hour).zfill(2)}:{str(start_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: End time:   {str(end_year).zfill(4)}-{str(end_month).zfill(2)}-{str(end_day).zfill(2)} {str(end_hour).zfill(2)}:{str(end_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: Data directory: {data_dir}\nDATA: Save directory: {save_dir}'''
                      f'''\nDATA: Args to pass: {cmd_args}\nDATA: Points to plot: {len(points_list)}''')
            if debug:
                print(f'DEBUG: Point data: \n{points_list}')

            start_date = datetime.datetime(start_year, start_month, start_day, start_hour, start_minute)
            end_date = datetime.datetime(end_year, end_month, end_day, end_hour, end_minute)

            ###PROCESSING

            data_period = pd.period_range(start=start_date, end=end_date, freq="1H")

            num_files = 0
            tot_time = 0
            plot_times = []
            tot_files = len(points_list)
            prev_points = []

            for point in points_list:
                point_lat = point[0]
                point_lon = point[1]
                point_alt = point[2]
                point_time = point[3]

                if verbose:
                    print(f"PROCESSING: Selected point ({point_lat}, {point_lon})")

                lowest_sec = 999999999
                sel_time = point_time

                for data_time in data_period:
                    delta_this_time = data_time.to_timestamp() - point_time
                    sec_diff = abs(delta_this_time.total_seconds())
                    if sec_diff < lowest_sec:
                        lowest_sec = sec_diff
                        sel_time = data_time.to_timestamp()

                sel_year = sel_time.year
                sel_month = sel_time.month
                sel_day = sel_time.day
                sel_hour = sel_time.hour

                if debug:
                    print(f"DEBUG: Selected time: {sel_time}")
                    print(f"DEBUG: Point time: {point_time}")
                    print(f"DEBUG: Seconds difference: {lowest_sec}")

                file_name = f"MERGED_access-c.{str(sel_year).zfill(4)}{str(sel_month).zfill(2)}{str(sel_day).zfill(2)}00.0{str(sel_hour).zfill(2)}.nc"

                file_path = os.path.join(data_dir, file_name)

                title = point_time.strftime("FLT%Y%m%d_%H%M%S")

                if not os.path.isfile(file_path):
                    if verbose:
                        print("PROCESSING: No files found for time. Skipping...")
                    continue
                else:
                    if verbose:
                        print(f"PROCESSING: Files found for time. Starting processor...")
                    status, time, model_point = access.model_sounding_raobcsv(file_path, save_dir, 
                                                                     			point_lat, point_lon, prev_points,
                                                                     			sounding_title=title, **cmd_args)

                    num_files += status
                    tot_time += time
                    plot_times += [time]
                    prev_points += [model_point]

                time_remaining = calc_time_remaining(tot_files, num_files, plot_times)
                print(f'{num_files}/{tot_files} soundings created. Est. time remaining: {time_remaining}', end=endchar)

            ###POSTPROCESSING

            tot_time_str = calc_total_time(tot_time)
            print(f'Finished creating {num_files} soundings. Total time: {tot_time_str}         ')

    if platform == "hrrr":
        platform_abr = "HRRR"

        if product == "download":
            print("Starting HRRR downloader...")

            ###INPUT PARSING

            save_dir = cmd.save_dir.text.strip()

            start_year = int(cmd.start_time.attrs['year'])
            start_month = int(cmd.start_time.attrs['month'])
            start_day = int(cmd.start_time.attrs['day'])
            start_hour = int(cmd.start_time.attrs['hour'])
            start_minute = int(cmd.start_time.attrs['minute'])

            end_year = int(cmd.end_time.attrs['year'])
            end_month = int(cmd.end_time.attrs['month'])
            end_day = int(cmd.end_time.attrs['day'])
            end_hour = int(cmd.end_time.attrs['hour'])
            end_minute = int(cmd.end_time.attrs['minute'])

            cmd_args = cmd.cmd_args.attrs

            if verbose:
                print(f'''DATA: Input data info below\nDATA: Platform: {platform}\nDATA: Product: {product}'''
                      f'''\nDATA: Start time: {str(start_year).zfill(4)}-{str(start_month).zfill(2)}-{str(start_day).zfill(2)} {str(start_hour).zfill(2)}:{str(start_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: End time:   {str(end_year).zfill(4)}-{str(end_month).zfill(2)}-{str(end_day).zfill(2)} {str(end_hour).zfill(2)}:{str(end_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: Save directory: {save_dir}\nDATA: Args to pass: {cmd_args}''')

            start_date = datetime.datetime(start_year, start_month, start_day, start_hour, start_minute)
            end_date = datetime.datetime(end_year, end_month, end_day, end_hour, end_minute)

            ###PROCESSING

            time_period = pd.period_range(start=start_date, end=end_date, freq="1H")

            if debug:
                print("PROCESSING: Time range: ")
                print(time_period)

            num_files = 0
            tot_time = 0
            plot_times = []
            tot_files = len(time_period)

            for time in time_period:
                year = time.year
                month = time.month
                day = time.day
                hour = time.hour
                minute = time.minute

                if verbose:
                    print(f"PROCESSING: Downloading time {str(year).zfill(4)}-{str(month).zfill(2)}-{str(day).zfill(2)} {str(hour).zfill(2)}:{str(minute).zfill(2)}:00 UTC")

               	status, time, path = hrrr.download(save_dir, year, month, day, hour, **cmd_args)

                num_files += status
                tot_time += time
                plot_times += [time]

                time_remaining = calc_time_remaining(tot_files, num_files, plot_times)
                print(f'{num_files}/{tot_files} files downloaded. Est. time remaining: {time_remaining}', end=endchar)

            ###POSTPROCESSING

            tot_time_str = calc_total_time(tot_time)
            print(f'Finished downloading {num_files} files. Total time: {tot_time_str}         ')

            # xml = lxml.builder.ElementMaker()
            # output_cmd_template = xml.command(xml.info(platform=platform), 
            #                                   xml.params(
            #                                              xml.start_time(year=start_year, month=start_month, 
            #                                                             day=start_day, hour=start_hour, 
            #                                                             minute=start_minute),
            #                                              xml.end_time(year=end_year, month=end_month, 
            #                                                           day=end_day, hour=end_hour, 
            #                                                           minute=end_minute),
            #                                              xml.data_dir(save_dir)
            #                                              )
            #                                   )

            # output_cmd_name = platform_abr + "_" + datetime.now().strftime("%Y%m%d_%h%M%S") + "_data_cmdbase.xml"
            # output_cmd_template.write(os.path.join(os.path.dirname(save_dir), ''))

        if product == "model_sounding_raobcsv":
            print("Starting HRRR Model Sounding (for RAOB) generator...")

            ###INPUT PARSING

            data_dir = cmd.data_dir.text.strip()
            save_dir = cmd.save_dir.text.strip()

            start_year = int(cmd.start_time.attrs['year'])
            start_month = int(cmd.start_time.attrs['month'])
            start_day = int(cmd.start_time.attrs['day'])
            start_hour = int(cmd.start_time.attrs['hour'])
            start_minute = int(cmd.start_time.attrs['minute'])

            end_year = int(cmd.end_time.attrs['year'])
            end_month = int(cmd.end_time.attrs['month'])
            end_day = int(cmd.end_time.attrs['day'])
            end_hour = int(cmd.end_time.attrs['hour'])
            end_minute = int(cmd.end_time.attrs['minute'])

            points_list = []
            for point in cmd.points.children:
                if not isinstance(point, NavigableString):
                    lat = float(point.attrs['lat'])
                    lon = float(point.attrs['lon'])
                    year = int(point.attrs['year'])
                    month = int(point.attrs['month'])
                    day = int(point.attrs['day'])
                    hour = int(point.attrs['hour'])
                    minute = int(point.attrs['minute'])
                    second = int(point.attrs['second'])
                    alt_ft = float(point.attrs['alt_ft'])

                    timestamp = pd.Timestamp(year=year, month=month, day=day, hour=hour, minute=minute, second=second)
                    points_list.append((lat, lon, alt_ft, timestamp))

            cmd_args = cmd.cmd_args.attrs

            if verbose:
                print(f'''DATA: Input data info below\nDATA: Platform: {platform}\nDATA: Product: {product}'''
                      f'''\nDATA: Start time: {str(start_year).zfill(4)}-{str(start_month).zfill(2)}-{str(start_day).zfill(2)} {str(start_hour).zfill(2)}:{str(start_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: End time:   {str(end_year).zfill(4)}-{str(end_month).zfill(2)}-{str(end_day).zfill(2)} {str(end_hour).zfill(2)}:{str(end_minute).zfill(2)}:00 UTC'''
                      f'''\nDATA: Data directory: {data_dir}\nDATA: Save directory: {save_dir}'''
                      f'''\nDATA: Args to pass: {cmd_args}\nDATA: Points to plot: {len(points_list)}''')
            if debug:
                print(f'DEBUG: Point data: \n{points_list}')

            start_date = datetime.datetime(start_year, start_month, start_day, start_hour, start_minute)
            end_date = datetime.datetime(end_year, end_month, end_day, end_hour, end_minute)

            ###PROCESSING

            data_period = pd.period_range(start=start_date, end=end_date, freq="1H")

            num_files = 0
            tot_time = 0
            plot_times = []
            tot_files = len(points_list)
            prev_points = []

            for point in points_list:
                point_lat = point[0]
                point_lon = point[1]
                point_alt = point[2]
                point_time = point[3]

                if verbose:
                    print(f"PROCESSING: Selected point ({point_lat}, {point_lon})")

                lowest_sec = 999999999
                sel_time = point_time

                for data_time in data_period:
                    delta_this_time = data_time.to_timestamp() - point_time
                    sec_diff = abs(delta_this_time.total_seconds())
                    if sec_diff < lowest_sec:
                        lowest_sec = sec_diff
                        sel_time = data_time.to_timestamp()

                sel_year = sel_time.year
                sel_month = sel_time.month
                sel_day = sel_time.day
                sel_hour = sel_time.hour

                if debug:
                    print(f"DEBUG: Selected time: {sel_time}")
                    print(f"DEBUG: Point time: {point_time}")
                    print(f"DEBUG: Seconds difference: {lowest_sec}")

                file_name = f"hrrr.{sel_year}{str(sel_month).zfill(2)}{str(sel_day).zfill(2)}.t{str(sel_hour).zfill(2)}z.wrfprsf00.grib2"

                file_path = os.path.join(data_dir, file_name)

                title = point_time.strftime("FLT%Y%m%d_%H%M%S")

                if not os.path.isfile(file_path):
                    if verbose:
                        print("PROCESSING: No files found for time. Skipping...")
                    continue
                else:
                    if verbose:
                        print(f"PROCESSING: Files found for time. Starting processor...")
                    status, time, model_point = hrrr.model_sounding_raobcsv(file_path, save_dir, 
                                                                     			point_lat, point_lon, prev_points,
                                                                     			sounding_title=title, **cmd_args)

                    num_files += status
                    tot_time += time
                    plot_times += [time]
                    prev_points += [model_point]

                time_remaining = calc_time_remaining(tot_files, num_files, plot_times)
                print(f'{num_files}/{tot_files} soundings created. Est. time remaining: {time_remaining}', end=endchar)

            ###POSTPROCESSING

            tot_time_str = calc_total_time(tot_time)
            print(f'Finished creating {num_files} soundings. Total time: {tot_time_str}         ')


if __name__ == "__main__":

    print(" WEXLIB CMD LINE ".center(82, "-"))

    while True:

        cmd_file = "/Users/rpurciel/Documents/Test/HRRR/command_hrrr_model_sounding_raobcsv.xml"

        print("\nInput a file path to use a command file, or press any key to use default: ")
        print(f"'{cmd_file}'")

        new_file = input()
        if os.path.isfile(new_file):
            cmd_file = new_file
            print(f"Using file {cmd_file}")

        processor_selector(cmd_file)

        opt = input("Press any key to run another command, or type 'x' to exit...")

        if opt == 'x' or opt == 'X':
            break
        else:
            continue












