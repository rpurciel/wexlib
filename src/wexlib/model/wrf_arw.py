import sys
import os
import glob
from datetime import datetime
import ssl
import warnings

import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import pandas as pd
from wrf import getvar
from netCDF4 import Dataset

# sys.path.insert(0, '/Users/ryanpurciel/Development/wexlib/src')
# sys.path.insert(0, '/Users/rpurciel/Development/wexlib/src') #FOR TESTING ONLY!!!
# import wexlib.util.internal as internal

warnings.filterwarnings('ignore')

#sounding options
DEFAULT_LAT = 39.446030
DEFAULT_LON = -119.771627

DEF_FILE_SKIP_DUPLICATES = True

def str_to_bool(string):
    if string in ['true', 'True', 'TRUE', 't', 'T', 'yes', 'Yes', 'YES', 'y', 'Y']:
        return True
    elif string in ['false', 'False', 'FALSE', 'f', 'F', 'no', 'No', 'NO', 'n', 'N']:
        return False
    else:
        if string == True:
            return True
        elif string == False:
            return False
        else:
            return False #fallback to false

def clean_idx(directory):
    idx_files = glob.glob(os.path.join(directory, "*.idx"))

    if not idx_files:
        return False
    else:
        for file in idx_files:
            os.remove(file)
        return True

def plot_plan_view_hrrr(file_path, save_dir, level, variables, points, bbox, **kwargs):
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
    dest_path = "NOT YET IMPLEMENTED"

    elapsed_time = datetime.now() - start_time
    return 1, elapsed_time.total_seconds(), dest_path

def plot_cross_section_hrrr(file_path, save_dir, start_point, end_point, variables, points, top_level, **kwargs):
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
    dest_path = "NOT YET IMPLEMENTED"

    elapsed_time = datetime.now() - start_time
    return 1, elapsed_time.total_seconds(), dest_path

def model_sounding_raobcsv(file_path, save_path, sounding_lat, sounding_lon, points_to_ignore_list, **kwargs):
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

    start_time = datetime.now()

    if str_to_bool(kwargs.get('verbose')) == True:
        verbose = True
        print("INFO: VERBOSE mode turned ON")
    else:
        verbose = False

    if str_to_bool(kwargs.get('debug')) == True:
        debug = True
        verbose = True
        print("INFO: DEBUG mode turned ON")
    else:
        debug = False

    if debug:
        print("DEBUG: Kwargs passed:", kwargs)

    # if sounding_lon < 0:
    #     sounding_lon += 360

    #     if debug:
    #         print("DEBUG: Sounding longitude corrected")
    #         print(f"DEBUG: Original={sounding_lon - 360} New={sounding_lon}")

    file_skip_duplicates = DEF_FILE_SKIP_DUPLICATES
    for arg, value in kwargs.items():
        if arg == 'file_skip_duplicates':
            file_skip_duplicates = str_to_bool(value)

    if verbose and file_skip_duplicates:
        print("IGNORE: Skipping duplicate points turned ON")

    data = xr.open_dataset(file_path, engine='netcdf4')
    data_wrf = Dataset(file_path)

    wrf_latitude = getvar(data_wrf,"lat")
    wrf_longitude = getvar(data_wrf,"lon")

    time_str = str(data.Times.data[0])[1:].replace("_", " ")

    print(time_str)

    date_time = pd.Timestamp(time_str) #Grabbing the datetime object

    date_internal = date_time.strftime("%Y%m%d_%H%M%S")

    date = date_time.strftime("%Y-%m-%d %H:%M:%S")

    abslat = np.abs(wrf_latitude-sounding_lat)
    abslon = np.abs(wrf_longitude-sounding_lon)
    c = np.maximum(abslon, abslat)

    try:
        ([idx_y], [idx_x]) = np.where(c == np.min(c))
    except ValueError:
        idx_y = np.where(c == np.min(c))[0][0]
        idx_x = np.where(c == np.min(c))[1][0]

    point_data = data.sel(south_north=idx_y, west_east=idx_x)

    selected_point = (point_data.XLAT.data, point_data.XLONG.data)
    if file_skip_duplicates:
        if selected_point in points_to_ignore_list:
            
            if verbose:
                print(f"IGNORE: Skipping selected point {selected_point} because point was found in 'points to ignore' list!")

            elapsed_time = datetime.now() - start_time
            return 1, elapsed_time.total_seconds(), selected_point #1 because "point ignored successfully"

    if verbose:
        print("INFO: Starting interpolation...")
        print('INFO: Requested pt:', sounding_lat, sounding_lon)
        print('INFO: Nearest pt:', point_data.XLAT.data, point_data.XLONG.data)

    latitude_float = round(float(point_data.XLAT.data),2) #Converts the array to a float and rounds to stick into dataframe later
    longitude_float = round(float(point_data.XLONG.data),2) #Same as for longitude

    uwind = point_data.U.data
    vwind = point_data.V.data
    theta = point_data.T.data + 300 # Potential temperature
    press = point_data.P.data + point_data.PB.data 
    qvapor = point_data.QVAPOR.data
    qvapor = np.ma.masked_where(qvapor <0.0000000001, qvapor)
    T0 = 273.15
    referencePressure = 100000.0 # [Pa]
    epsilon = 0.622 # Rd / Rv
    
    # Temperatures in Celsius
    temperature = theta* np.power((press / referencePressure), 0.2854) - T0
    vaporPressure = press * qvapor / (epsilon + qvapor)
    dewPointTemperature = 243.5 / ((17.67 / np.log(vaporPressure * 0.01 / 6.112)) - 1.) #In celsius
    dewPointTemperature = np.ma.masked_invalid(dewPointTemperature)
    dewPointTemperature = dewPointTemperature.data
    
    #Single Arrays
    press = press[0,:]
    press = press/100
    tmpc = temperature [0,:]
    dptc = dewPointTemperature[0,:]
    PH = point_data.PH[0,:].data
    PHB = point_data.PHB[0,:].data

    #Surface/2m Parameters    
    presssurfacePa_pa = point_data.PSFC.data
    presssurfacePa = presssurfacePa_pa/100
    tmp2m = point_data.T2.data
    tmp2mc = tmp2m - 273.15 #Surface Temp in Kelvin
    hgt = (PH + PHB) / 9.81 #Geopotential Height
    qvapor2m = point_data.Q2.data
    vaporPressure2m = presssurfacePa_pa * qvapor2m / (epsilon + qvapor2m)
    dpt2mc = 243.5 / ((17.67 / np.log(vaporPressure2m * 0.01 / 6.112)) - 1.)
    hgtsurface = point_data.HGT.data
    hgt2m = hgtsurface + 2.
    
    uwindavg = []
    vwindavg = []
    
    for i in range(len(uwind[0,:])):
        avg = np.sum(uwind[0,i]) / len(uwind[0,0,:])
        uwindavg.append(avg)
    
    for i in range(len(vwind[0,:])):
        avg = np.sum(vwind[0,i]) / len(vwind[0,0,:])
        vwindavg.append(avg)
        
    #Convert u & v wind to wind speed
    #wspd = np.sqrt(np.square(uwind) + np.square(vwind))
    
    #Convert u & v winds (m/s) to kts
    uwindkts = np.dot(uwindavg, 1.94384)
    vwindkts = np.dot(vwindavg, 1.94384)

    #Create and combine separate data frames in main data frame
    df = pd.DataFrame(data=[press, tmpc, dptc, uwindkts, vwindkts, hgt])
    df_t = df.T
    main_df = df_t
        
    main_df = main_df.round(decimals=2)
    elev = round(float(hgt[0])) #Rounding surface elevation for dataframe

    if kwargs.get('sounding_title'):
        csv_name = kwargs.get('sounding_title')
        file_name = date.replace(":","").replace("-", "").replace(" ", "_") + "_" + csv_name + "_WRF_RAOB.csv"
    else:
        csv_name = "UNNAMED SOUNDING"
        file_name = date.replace(":","").replace("-", "").replace(" ", "_") + "_WRF_RAOB.csv"
    
    d = {0:['RAOB/CSV','DTG','LAT','LON','ELEV','MOISTURE','WIND','GPM','MISSING','RAOB/DATA','PRES'],
         1:[csv_name,date,latitude_float,longitude_float,elev,'TD','kts','MSL',-999,'','TEMP'],2:['','','N','W','m','','U/V','','','','TD'],3:['','','','','','','','','','','UU'],
         4:['','','','','','','','','','','VV'],5:['','','','','','','','','','','GPM']}
    df_header = pd.DataFrame(data=d)
    
    main_df = pd.concat([df_header,main_df],axis=0,ignore_index=True) #Combines the RAOB Header Format with the sounding data

    dest_path = os.path.join(save_path, file_name)

    main_df.to_csv(dest_path, index=False, header=False)

    if verbose:
        print("FILE: Saved File: " + file_name + " to " + save_path)

    elapsed_time = datetime.now() - start_time
    return 1, elapsed_time.total_seconds(), selected_point

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

    # input_file_path = "/Users/rpurciel/WeatherExtreme Ltd Dropbox/Ryan Purciel/Voltitude/WRF Tests/TEST3nudging_data/wrfout_d03_2023-09-13_20:00:00"

    # input_save_dir = "/Users/rpurciel/WeatherExtreme Ltd Dropbox/Ryan Purciel/Voltitude/WRF Tests/TEST3nudging_comparison/Soundings"

    # title = "NUDGED_BDM15DS22"

    # lat = -26.0916134

    # lon = 16.7566683

    # _, _, _ = model_sounding_raobcsv(input_file_path, input_save_dir, lat, lon, [], sounding_title=title, debug=True)

    # input_file_path = "/Users/rpurciel/WeatherExtreme Ltd Dropbox/Ryan Purciel/Voltitude/WRF Tests/TEST3nudging_data/wrfout_d03_2023-09-15_16:30:00"
    # print(input_file_path)

    # input_save_dir = "/Users/rpurciel/WeatherExtreme Ltd Dropbox/Ryan Purciel/Voltitude/WRF Tests/TEST3nudging_comparison/Soundings"

    # title = "NUDGED_BDM16DS13"

    # lat = -25.941905

    # lon = 16.3191816

    # _, _, _ = model_sounding_raobcsv(input_file_path, input_save_dir, lat, lon, [], sounding_title=title, debug=True)

    # input_file_path = "/Users/rpurciel/WeatherExtreme Ltd Dropbox/Ryan Purciel/Voltitude/WRF Tests/TEST2_data/wrfout_d03_2023-09-13_20:00:00"

    # input_save_dir = "/Users/rpurciel/WeatherExtreme Ltd Dropbox/Ryan Purciel/Voltitude/WRF Tests/TEST3nudging_comparison/Soundings"

    # title = "BDM15DS22"

    # lat = -26.0916134

    # lon = 16.7566683

    # _, _, _ = model_sounding_raobcsv(input_file_path, input_save_dir, lat, lon, [], sounding_title=title, debug=True)

    # input_file_path2 = "/Users/rpurciel/WeatherExtreme Ltd Dropbox/Ryan Purciel/Voltitude/WRF Tests/TEST2_data/wrfout_d03_2023-09-15_16:30:00"
    # print(input_file_path)

    # input_save_dir = "/Users/rpurciel/WeatherExtreme Ltd Dropbox/Ryan Purciel/Voltitude/WRF Tests/TEST3nudging_comparison/Soundings"

    # title = "BDM16DS13"

    # lat = -25.941905

    # lon = 16.3191816

    # _, _, _ = model_sounding_raobcsv(input_file_path2, input_save_dir, lat, lon, [], sounding_title=title, debug=True)

    ##BDM11DS29

    ingest_file_dir = '/Users/rpurciel/Documents/Voltitude/FINAL WRF DATA'
    control_file_dir = '/Users/rpurciel/Documents/Voltitude/FINAL WRF CONTROL DATA'
    # test_file_dir = '/Users/rpurciel/Documents/Voltitude/WRF TEST DATA'

    save_dir = '/Users/rpurciel/Documents/Voltitude/Final WRF Comparison/Soundings/TEST/'

    modes = ['CONTROL', 'INGEST']

    sondes = [('BDM11DS28', -25.4275, 16.4557499, 'wrfout_d03_2023-09-08_18:00:00'),
              ('BDM11DS29', -25.61189, 16.1760499, 'wrfout_d03_2023-09-08_20:00:00'),
              ('BDM15DS22', -26.0916134, 16.7566683, 'wrfout_d03_2023-09-13_20:00:00'),
              ('BDM16DS13', -25.941905, 16.3191816, 'wrfout_d03_2023-09-15_17:00:00'),
              ('BDM13DS02', -25.223895, 17.4897266, 'wrfout_d03_2023-09-18_14:00:00'),
              ('BDM13DS03', -26.527885, 17.5203299, 'wrfout_d03_2023-09-18_18:00:00'),
              ('BDM17DS08', -26.733505, 17.2086933, 'wrfout_d03_2023-09-19_18:00:00'),
              ('BDM17DS14', -37.5441567,15.9044683, 'wrfout_d03_2023-09-20_20:30:00'),
              ('BDM17DS09', -31.2030967, 16.6450866, 'wrfout_d03_2023-09-20_07:30:00')]

    #sondes = [('BDM11DS29', -25.61189, 16.1760499, 'wrfout_d03_2023-09-09_08:00:00')]

    for sonde in sondes:
        sonde_id = sonde[0]
        lat = sonde[1]
        lon = sonde[2]
        file_name = sonde[3]

        for mode in modes:

            sonde_name = f"{sonde_id}_{mode}"

            if mode == 'INGEST':
                file_path = os.path.join(ingest_file_dir, file_name)
                dest_path = os.path.join(save_dir, mode)
            elif mode == 'TEST':
                file_path = os.path.join(test_file_dir, file_name)
                dest_path = os.path.join(save_dir, mode)
            else:
                file_path = os.path.join(control_file_dir, file_name)
                dest_path = os.path.join(save_dir, mode)

            if not os.path.exists(dest_path):
                os.makedirs(dest_path)

            _, _, _ = model_sounding_raobcsv(file_path, dest_path, lat, lon, [], sounding_title=sonde_name, debug=True)



        









