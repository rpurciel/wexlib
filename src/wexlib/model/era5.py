import sys
import os
import glob
from datetime import datetime

import xarray as xr
import cfgrib
import numpy as np
import pandas as pd
import cdsapi

#sounding options
DEF_FILE_SKIP_DUPLICATES = False

#download options
DEF_DL_BBOX = None
#all pressure levels available
DEF_DL_PRESSURE_LEVELS = [
    '1', '2', '3',
    '5', '7', '10',
    '20', '30', '50',
    '70', '100', '125',
    '150', '175', '200',
    '225', '250', '300',
    '350', '400', '450',
    '500', '550', '600',
    '650', '700', '750',
    '775', '800', '825',
    '850', '875', '900',
    '925', '950', '975',
    '1000',
    ]
#all variables available
DEF_DL_UA_VARIABLES = [
    'divergence', 'fraction_of_cloud_cover', 'geopotential',
    'ozone_mass_mixing_ratio', 'potential_vorticity', 'relative_humidity',
    'specific_cloud_ice_water_content', 'specific_cloud_liquid_water_content', 'specific_humidity',
    'specific_rain_water_content', 'specific_snow_water_content', 'temperature',
    'u_component_of_wind', 'v_component_of_wind', 'vertical_velocity',
    'vorticity',
    ]
DEF_DL_SFC_VARIABLES = [
    '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_dewpoint_temperature',
    '2m_temperature', 'geopotential', 'surface_pressure',
    ]

DEF_ERA5_VARIABLES = {
    'd' : 'Divergence',
    'cc' : 'Fraction of cloud cover',
    'z' : 'Geopotential',
    'o3' : 'Ozone mass mixing ratio',
    'pv' : 'Potential vorticity',
    'r' : 'Relative humidity',
    'ciwc' : 'Specific cloud ice water content',
    'clwc' : 'Specific cloud liquid water content',
    'q' : 'Specific humidity',
    'crwc' : 'Specific rain water content',
    'cswc' : 'Specific snow water content',
    't' : 'Temperature',
    'u' : 'U component of wind',
    'v' : 'V component of wind',
    'w' : 'Vertical velocity',
    'vo' : 'Vorticity (relative)',
    }

def download(save_dir, year, month, day, hour, bbox=DEF_DL_BBOX, **kwargs):
    """
    Downloads a single model file to a local directory. 

    Inputs: Directory to save model output files to, and year, month,
            day, and hour to download model file for.

    Returns: Success code, time (in seconds) for function to run,
             path to file
    """
    start_time = datetime.now()

    if bool(kwargs.get('verbose')) == True:
        verbose = True
        print("INFO: VERBOSE mode turned ON")
    else:
        verbose = False

    if bool(kwargs.get('debug')) == True:
        debug = True
        verbose = True
        print("INFO: DEBUG mode turned ON")
    else:
        debug = False

    if debug:
        print("DEBUG: Kwargs passed:", kwargs)

    dl_pressure_levels = DEF_DL_PRESSURE_LEVELS
    dl_ua_variables = DEF_DL_UA_VARIABLES
    dl_sfc_variables = DEF_DL_SFC_VARIABLES
    for arg, value in kwargs.items():
        if arg == 'dl_pressure_levels':
            dl_pressure_levels = value
        if arg == 'dl_ua_variables':
            dl_ua_variables = value
        if arg == 'dl_sfc_variables':
            dl_sfc_variables = value

    if debug:
        print("DEBUG: CDS API quiet mode turned OFF")
        cds = cdsapi.Client()
    else:
        cds = cdsapi.Client(quiet=True)

    ua_fname = f"{str(year).zfill(4)}_{str(month).zfill(2)}_{str(day).zfill(2)}_{str(hour).zfill(2)}00_UA_ERA5.grib"
    sfc_fname = f"{str(year).zfill(4)}_{str(month).zfill(2)}_{str(day).zfill(2)}_{str(hour).zfill(2)}00_SFC_ERA5.grib"

    ua_dest_path = os.path.join(save_dir, ua_fname)
    sfc_dest_path = os.path.join(save_dir, sfc_fname)

    hour_str = f"{str(hour).zfill(2)}:00:00"

    if bbox:
        bbox_NWSE = [bbox[0], bbox[3], bbox[2], bbox[1]]
        dl_bbox = bbox_NWSE

        if verbose:
            print(f"INFO: Using bounding box")

        ua_request = {
            'product_type': 'reanalysis',
            'format': 'grib',
            'variable': dl_ua_variables,
            'pressure_level': dl_pressure_levels,
            'year': str(year).zfill(4),
            'month': str(month).zfill(2),
            'day': str(day).zfill(2),
            'time': hour_str,
            'area': dl_bbox
        }

        sfc_request = {
            'product_type': 'reanalysis',
            'format': 'grib',
            'variable': dl_sfc_variables,
            'year': str(year).zfill(4),
            'month': str(month).zfill(2),
            'day': str(day).zfill(2),
            'time': hour_str,
            'area': dl_bbox
        }

    else:
        ua_request = {
            'product_type': 'reanalysis',
            'format': 'grib',
            'variable': dl_ua_variables,
            'pressure_level': dl_pressure_levels,
            'year': str(year).zfill(4),
            'month': str(month).zfill(2),
            'day': str(day).zfill(2),
            'time': hour_str
        }

        sfc_request = {
            'product_type': 'reanalysis',
            'format': 'grib',
            'variable': dl_sfc_variables,
            'year': str(year).zfill(4),
            'month': str(month).zfill(2),
            'day': str(day).zfill(2),
            'time': hour_str
        }

    if verbose:
        print(f"UPPER AIR: Data request: {ua_request}")
        print(f"UPPER AIR: Starting download...")

    cds.retrieve('reanalysis-era5-pressure-levels', ua_request, ua_dest_path)

    if verbose:
        print("UPPER AIR: Finished!")
        print(f"SURFACE: Data request: {sfc_request}")
        print(f"SURFACE: Starting download...")

    cds.retrieve('reanalysis-era5-single-levels', sfc_request, sfc_dest_path)

    if verbose:
        print("SURFACE: Finished!")

    elapsed_time = datetime.now() - start_time
    return 1, elapsed_time.total_seconds(), ua_dest_path, sfc_dest_path

def plot_plan_view(file_path, save_dir, time_index, level, variables, points, **kwargs):
    """
    Using a bounding box, plots a list of variables and (opt.) points for a single level.

    Bounding box is specified via a tuple:
    (ll corner lat, ll corner lon, ur corner lat, ur corner lon)

    Variables are specified via a list of short or long names:
    ['t', 'u', 'v', ...] OR ['temp', 'uwind', 'vwind', ...]
    -> These are then compared to a dictionary that converts
       into the appropriate short name for the model at hand.

    Points are specified as a list of tuples, including label:
    [(lat1, lon1, label1), (lat2, lon2, label2), ...]

    Level is specified as a level in hPa, and is interpolated to the nearest
    model level if that exact level is not allowed in the modelling.
    % (TODO: add ability to interpolate to specified level)

    Returns: Success code, time (in seconds) for function to run,
             path to file
    """
    start_time = datetime.now()


    # base_data = xr.open_dataset(file_path, engine="cfgrib")

    # if time_index <= len(base_data.time) and time_index > 0:
    #     seltime_data = base_data.isel(time=time_index)
    # else:
    #     #error: time index is not valid
    #     error_str = "Error: Time index is not valid (" + str(time_index) + " not within range 0 to " + str(len(base_data.time)) + ")" 
    #     elapsed_time = datetime.now() - start_time
    #     return 0, elapsed_time.total_seconds(), error_str

    # seltime_sellevel_data = seltime_data.sel(isobaricInhPa=level)

    # data = seltime_sellevel_data

    # for variable in DEF_ERA5_VARIABLES.keys():

    elapsed_time = datetime.now() - start_time
    return 1, elapsed_time.total_seconds(), dest_path

def plot_cross_section(file_path, save_dir, start_point, end_point, variables, points, top_level, **kwargs):
    """
    Using a start point & end point, plot variables and (opt.) points (as vlines) to a top level.

    Start and end points are specified as a tuple:
    (lat, lon)

    Variables are specified via a list of short or long names:
    ['t', 'u', 'v', ...] OR ['temp', 'uwind', 'vwind', ...]
    -> These are then compared to a dictionary that converts
       into the appropriate short name for the model at hand.

    Points are specified as a list of tuples, including label:
    [(lat1, lon1, label1), (lat2, lon2, label2), ...]
    * If these points do not have a lat/lon that is exactly
      on the line of the cross-section, they will not be
      plotted.
    % (TODO: add ability to plot points even if off the line)

    Top level is specified as a level in hPa. Default is 100 hPa,
    and it cannot be set lower than the bottom level.

    (Bottom level is by default set to 1000 hPa, but can be modified
    via kwarg bot_level=xxxx.)    

    Returns: Success code, time (in seconds) for function to run,
             path to file
    """
    start_time = datetime.now()

    elapsed_time = datetime.now() - start_time
    return 1, elapsed_time.total_seconds(), dest_path

def model_sounding_raobcsv(ua_file_path, sfc_file_path, save_path, sounding_lat, sounding_lon, points_to_ignore_list, **kwargs):
    """
    Using a lat/lon, generates a CSV sounding for use in RAOB.

    Latitude and longitude are specified as decimal floats, with
    the default value set in this file (if none are specified).

    Returns: Success code, time (in seconds) for function to run,
             path to output file
    """

    start_time = datetime.now()

    if bool(kwargs.get('verbose')) == True:
        verbose = True
        print("INFO: VERBOSE mode turned ON")
    else:
        verbose = False

    if bool(kwargs.get('debug')) == True:
        debug = True
        verbose = True
        print("INFO: DEBUG mode turned ON")
    else:
        debug = False

    if debug:
        print("DEBUG: Kwargs passed:", kwargs)

    if sounding_lon < 0 :
        sounding_lon += 360

        if debug:
            print("DEBUG: Sounding longitude corrected")
            print(f"DEBUG: Original={sounding_lon - 360} New={sounding_lon}")

    file_skip_duplicates = DEF_FILE_SKIP_DUPLICATES
    for arg, value in kwargs.items():
        if arg == 'file_skip_duplicates':
            file_skip_duplicates = internal.str_to_bool(value)

    if verbose and file_skip_duplicates:
    	print("IGNORE: Skipping duplicate points turned ON")

    tz_ua_ds = xr.open_dataset(ua_file_path, engine='cfgrib', backend_kwargs={'filter_by_keys':{'typeOfLevel': 'isobaricInhPa'}})

    date_time = pd.Timestamp(tz_ua_ds.time.data) #Grabbing the datetime object

    date_internal = date_time.strftime("%Y%m%d_%H%M%S")

    date = date_time.strftime("%Y-%m-%d %H:%M:%S")

    # tz_ua_ds = tz_ua_ds.sel(time=date_internal)

    ##Temps
    
    point_ds = tz_ua_ds.sel(longitude=sounding_lon, latitude=sounding_lat, method= 'nearest')

    selected_point = (point_ds.latitude.data, point_ds.longitude.data)
    if file_skip_duplicates:
	    if selected_point in points_to_ignore_list:
	        
	        if verbose:
	            print(f"IGNORE: Skipping selected point {selected_point} because point was found in 'points to ignore' list!")

	        elapsed_time = datetime.now() - start_time
	        return 1, elapsed_time.total_seconds(), selected_point #1 because "point ignored successfully"

    if verbose:
        print("UPPER AIR TEMPS: Starting interpolation...")
        print('INFO: Requested pt:', sounding_lat, sounding_lon)
        print('INFO: Nearest pt:', point_ds.latitude.data, point_ds.longitude.data)
    
    latitude_float = round(float(point_ds.latitude.data),2) #Converts the array to a float and rounds to stick into dataframe later
    longitude_float = round(float(point_ds.longitude.data),2) #Same as for longitude
    
    # These are the profiles you want...
    press = point_ds.isobaricInhPa.data
    tmp = point_ds.t.data
    hgt = point_ds.z.data
    
    #Convert Kelvin temps to C
    tmpc = tmp -273.15
    hgt = hgt / 9.80665
    
    ##Winds
    wind_ua_ds = xr.open_dataset(ua_file_path, engine='cfgrib', backend_kwargs={'filter_by_keys':{'typeOfLevel': 'isobaricInhPa', 'units': 'm s**-1'}, 'errors':'ignore'})

    # wind_ua_ds = wind_ua_ds.sel(time=date_internal)

    point_dsw = wind_ua_ds.sel(longitude=sounding_lon, latitude=sounding_lat, method= 'nearest')

    if verbose:
        print("UPPER AIR WINDS: Starting interpolation...")
        print('INFO: Requested pt:', sounding_lat, sounding_lon)
        print('INFO: Nearest pt:', point_dsw.latitude.data, point_dsw.longitude.data)

    uwind = point_dsw.u.data
    vwind = point_dsw.v.data

    uwindkts = np.dot(uwind, 1.94384)
    vwindkts = np.dot(vwind, 1.94384)

    ##Relative Humidity

    rh_ua_ds = xr.open_dataset(ua_file_path, engine='cfgrib', backend_kwargs={'filter_by_keys':{'typeOfLevel': 'isobaricInhPa', 'shortName': 'r'}})

    # rh_ua_ds = rh_ua_ds.sel(time=date_internal)

    point_dsrh = rh_ua_ds.sel(longitude=sounding_lon, latitude=sounding_lat, method= 'nearest')

    if verbose:
        print("UPPER AIR RELH: Starting interpolation...")
        print('INFO: Requested pt:', sounding_lat, sounding_lon)
        print('INFO: Nearest pt:', point_dsrh.latitude.data, point_dsrh.longitude.data)

    relativehumidity = point_dsrh.r.data

    A = 17.27
    B = 237.7

    dwptc = B * (np.log(relativehumidity/100.) + (A*tmpc/(B+tmpc))) / (A-np.log(relativehumidity/100.)-((A*tmpc)/(B+tmpc)))
    
    
    ####Redo Process for Surface Layer####
    
    #Open the grib2 file with xarray and cfgrib
    
    sfc_ds = xr.open_dataset(sfc_file_path, engine='cfgrib')
    # sfc_ds = sfc_ds.sel(time=date_internal)
    
    point_d2m = sfc_ds.sel(longitude=sounding_lon, latitude=sounding_lat, method= 'nearest')
    
    if verbose:
        print("SURFACE: Starting interpolation...")
        print('INFO: Requested pt:', sounding_lat, sounding_lon)
        print('INFO: Nearest pt:', point_d2m.latitude.data, point_d2m.longitude.data)
    
    pressur2m = point_d2m.sp.data
    tmp2m = point_d2m.t2m.data
    dpt2m = point_d2m.d2m.data
    hgt2m = point_d2m.z.data
    uwind_10m = point_d2m.u10.data
    vwind_10m = point_d2m.v10.data
    
    #Convert Kelvin temps to C
    tmp2mc = tmp2m -273.15
    dpt2mc = dpt2m -273.15
    
    #Convert Pa Pressure to hpa
    pressur2mPa = pressur2m * 0.01
    
    #Convert Geopotential to Geopotential Height
    hgt2m = hgt2m / 9.80665
    
    #Convert u & v winds (m/s) to kts
    uwindkts_10m = np.dot(uwind_10m, 1.94384)
    vwindkts_10m = np.dot(vwind_10m, 1.94384)
    
    #Create and combine separate data frames in main data frame
    df = pd.DataFrame(data=[press, tmpc, dwptc, uwindkts, vwindkts, hgt])

    d2 = pd.DataFrame(data=[pressur2mPa,tmp2mc,dpt2mc,uwindkts_10m,vwindkts_10m,hgt2m])

    d2 = d2.T

    df_t = df.T
    main_df = pd.concat([d2,df_t],axis=0,ignore_index=True)
    
    #Fill Nan arrays with -999 and get rid of Nan arrays for U & V Columns
    main_df[2].fillna(-999, inplace=True)
    main_df[3].fillna('', inplace=True)
    main_df[4].fillna('', inplace=True)        

    #Removes the pressure layers below the surface of the ground
    main_df = main_df[main_df[0] <= pressur2mPa]
    
    main_df = main_df.round(decimals=2)
    elev = round(float(hgt2m)) #Rounding surface elevation for datatframe

    if kwargs.get('sounding_title'):
        csv_name = kwargs.get('sounding_title')
        file_name = date_internal + "_" + csv_name + "_ERA5_RAOB.csv"
    else:
        csv_name = "UNNAMED SOUNDING"
        file_name = date_internal + "_ERA5_RAOB.csv"
    
    d = {0:['RAOB/CSV','DTG','LAT','LON','ELEV','MOISTURE','WIND','GPM','MISSING','RAOB/DATA','PRES'],
         1:[csv_name,date,latitude_float,longitude_float,elev,'TD','kts','MSL',-999,'','TEMP'],2:['','','N','W','','','U/V','','','','TD'],3:['','','','','','','','','m','','UU'],
         4:['','','','','','','','','','','VV'],5:['','','','','','','','','','','GPM']}
    df_2 = pd.DataFrame(data=d)
    
    main_df = pd.concat([df_2,main_df],axis=0,ignore_index=True) #Combines the RAOB Header Format with the sounding data
    
    # for idx_files in glob.glob(working_dir+"*.idx"):
    #     os.remove(idx_files)
    
    dest_path = os.path.join(save_path, file_name)

    main_df.to_csv(dest_path, index=False, header=False)

    if verbose:
        print("FILE: Saved File: " + file_name + " to " + save_path)

    elapsed_time = datetime.now() - start_time
    return 1, elapsed_time.total_seconds(), selected_point

def _calculate_variable_era5(variable_short_name, input_data):
    """Internal function: given a short name, return a calculated variable using input data.

    This works strictly with defined calculations, and only with standard (as defined
    for this library) units.
    """

def _convert_natural_name_to_short_name_era5(natural_name):
    """Internal function: given a natural variable name, return a short name that works for the specified model.

    This works with certain natural names, and only for defined variables.
    """

def time_remaining_calc(tot_items, processed_items, proc_times_list):

    if processed_items <= 1:
        avg_time = 0
    else:
        avg_time = sum(proc_times_list) / len(proc_times_list)

    time_remaining = (avg_time * (tot_items - processed_items))/3600 #in hours
    tr_hours = time_remaining
    tr_minutes = (time_remaining*60) % 60
    tr_seconds = (time_remaining*3600) % 60

    time_remaining_str = "{}:{}:{}".format(str(round(tr_hours)).zfill(2), str(round(tr_minutes)).zfill(2), str(round(tr_seconds)).zfill(2))

    return time_remaining_str

def total_time_calc(total_time_seconds):

    t_hours = total_time_seconds/3600
    t_minutes = (t_hours*60) % 60
    t_seconds = (t_hours*3600) % 60

    time_str = "{}:{}:{}".format(str(round(t_hours)).zfill(2), str(round(t_minutes)).zfill(2), str(round(t_seconds)).zfill(2))

    return time_str

if __name__ == "__main__":

    input_save_dir = "/Users/rpurciel/Documents/ERA5/Soundings"

    era5_dir = "/Users/rpurciel/Documents/ERA5/Cabo Verde Surface"

    sonde_lats = (16.4649199, 15.9492216, 
        16.6780999, 17.2468933, 17.5750033, 
        17.5512033, 16.2812833, 16.7492233, 
        16.1857866)

    sonde_lons = (-25.45447, -37.589945, 
        -31.22682, -26.75401, -26.499205, 
        -25.2049534, -25.9776217, -26.1555684, 
        -25.6654517)

    sonde_hrs = (18, 22, 8, 18, 18, 14, 17, 20, 20)

    sonde_ids = ('11DS28', '17DS14', 
        '17DS09', ' 17DS08', '13DS03',
        '13DS02', '16DS13', '15DS22',
        '11DS29')

    sonde_dates = (8, 20, 20, 19, 18, 18, 15, 13, 8)

    num_files = 0
    tot_time = 0
    plot_times = []

    tot_files = len(sonde_dates)

    for lat, lon, hr, date, name in zip(sonde_lats, sonde_lons, sonde_hrs, sonde_dates, sonde_ids):

        fname_base = "2023_09_" + str(date).zfill(2) + "_"
        fname_suffix = "_CV_Hourly_ERA5.grib"

        ua_fname = fname_base + "UA" + fname_suffix
        sfc_fname = fname_base + "SFC" + fname_suffix

        ua_file = os.path.join(era5_dir, ua_fname)
        sfc_file = os.path.join(era5_dir, sfc_fname)

        status, time, path = raob_csv_sounding_era5(ua_file, sfc_file, input_save_dir, hr, lat, lon, sounding_title=name, debug=False)

        num_files += status
        tot_time += time
        plot_times += [time]

        time_remaining = time_remaining_calc(tot_files, num_files, plot_times)

        print(f'{num_files}/{tot_files} soundings created. Est. time remaining: {time_remaining}', end='\r')

    tot_time_str = total_time_calc(tot_time)

    print(f'Finished creating {num_files} soundings. Total time: {tot_time_str}         ')






