import sys
import os
import glob
from datetime import datetime

import xarray as xr
import numpy as np
import pandas as pd
import cdsapi
import netCDF4

import wexlib.util.internal as internal

#sounding options
DEF_FILE_SKIP_DUPLICATES = False

def download(save_dir, year, month, day, hour, **kwargs):
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

    dest_path = "Downloading of ACCESS data not yet implemented!"

    elapsed_time = datetime.now() - start_time
    return 1, elapsed_time.total_seconds(), dest_path

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

def model_sounding_raobcsv(file_path, save_path, sounding_lat, sounding_lon, points_to_ignore_list, **kwargs):
    """
    Using a lat/lon, generates a CSV sounding for use in RAOB.

    Latitude and longitude are specified as decimal floats, with
    the default value set in this file (if none are specified).

    Returns: Success code, time (in seconds) for function to run,
             path to output file
    """

    start_time = datetime.now()

    if internal.str_to_bool(kwargs.get('verbose')) == True:
        verbose = True
        print("INFO: VERBOSE mode turned ON")
    else:
        verbose = False

    if internal.str_to_bool(kwargs.get('debug')) == True:
        debug = True
        verbose = True
        print("INFO: DEBUG mode turned ON")
    else:
        debug = False

    if debug:
        print("DEBUG: Kwargs passed:", kwargs)

    # if sounding_lon < 0 :
    #     sounding_lon += 360

    #     if debug:
    #         print("DEBUG: Sounding longitude corrected")
    #         print(f"DEBUG: Original={sounding_lon - 360} New={sounding_lon}")

    file_skip_duplicates = DEF_FILE_SKIP_DUPLICATES
    for arg, value in kwargs.items():
        if arg == 'file_skip_duplicates':
            file_skip_duplicates = internal.str_to_bool(value)

    if verbose and file_skip_duplicates:
    	print("IGNORE: Skipping duplicate points turned ON")

    data = xr.open_dataset(file_path, engine="netcdf4")

    date_time = pd.Timestamp(data.time.data[0]) #Grabbing the datetime object

    date_internal = date_time.strftime("%Y%m%d_%H%M%S")

    date = date_time.strftime("%Y-%m-%d %H:%M:%S")

    # tz_ua_ds = tz_ua_ds.sel(time=date_internal)

    ##Temps
    
    point_data = data.sel(lon=sounding_lon, lat=sounding_lat, method= 'nearest')

    selected_point = (point_data.lat.data, point_data.lon.data)
    if file_skip_duplicates:
	    if selected_point in points_to_ignore_list:
	        
	        if verbose:
	            print(f"IGNORE: Skipping selected point {selected_point} because point was found in 'points to ignore' list!")

	        elapsed_time = datetime.now() - start_time
	        return 1, elapsed_time.total_seconds(), selected_point #1 because "point ignored successfully"

    if verbose:
        print("INFO: Starting interpolation...")
        print('INFO: Requested pt:', sounding_lat, sounding_lon)
        print('INFO: Nearest pt:', point_data.lat.data, point_data.lon.data)
    
    latitude_float = round(float(point_data.lat.data),2) #Converts the array to a float and rounds to stick into dataframe later
    longitude_float = round(float(point_data.lon.data),2) #Same as for longitude
    
    # These are the profiles you want...
    press = point_data.lvl.data
    tmp = point_data.air_temp.data
    hgt = point_data.geop_ht.data
    
    #Convert Kelvin temps to C
    tmpc = tmp - 273.15
    hgt = hgt / 9.80665 #GPM to meters

    uwind = point_data.zonal_wnd.data
    vwind = point_data.merid_wnd.data

    uwindkts = np.dot(uwind, 1.94384) #m/s to knots
    vwindkts = np.dot(vwind, 1.94384)

    ##Relative Humidity

    relativehumidity = point_data.relhum.data

    A = 17.27
    B = 237.7

    dwptc = B * (np.log(relativehumidity/100.) + (A*tmpc/(B+tmpc))) / (A-np.log(relativehumidity/100.)-((A*tmpc)/(B+tmpc)))
    
    #Create and combine separate data frames in main data frame
    df = pd.DataFrame(data=[press, tmpc[0], dwptc[0], uwindkts[0], vwindkts[0], hgt[0]])

    df_t = df.T
    main_df = pd.concat([df_t], axis=0, ignore_index=True)
    
    #Fill Nan arrays with -999 and get rid of Nan arrays for U & V Columns
    main_df[2].fillna(-999, inplace=True)
    main_df[3].fillna('', inplace=True)
    main_df[4].fillna('', inplace=True)        

    # #Removes the pressure layers below the surface of the ground
    # main_df = main_df[main_df[0] <= pressur2mPa]
    
    main_df = main_df.round(decimals=2)
    #elev = round(float(hgt[0])) #Rounding surface elevation for datatframe
    elev = 0

    if kwargs.get('sounding_title'):
        csv_name = kwargs.get('sounding_title')
        file_name = date_internal + "_" + csv_name + "_ACCESS_RAOB.csv"
    else:
        csv_name = "UNNAMED SOUNDING"
        file_name = date_internal + "_ACCESS_RAOB.csv"
    
    d = {0:['RAOB/CSV','DTG','LAT','LON','ELEV','MOISTURE','WIND','GPM','MISSING','RAOB/DATA','PRES'],
         1:[csv_name,date,latitude_float,longitude_float,elev,'TD','kts','MSL',-999,'','TEMP'],2:['','','N','W','','','U/V','','','','TD'],3:['','','','','','','','','','','UU'],
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

if __name__ == "__main__":

    pass






