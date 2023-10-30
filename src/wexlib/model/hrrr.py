import sys, os, glob
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import ssl
import urllib.request

#sounding options
DEFAULT_LAT = 39.446030
DEFAULT_LON = -119.771627

def download_hrrr(save_dir, year, month, day, hour, **kwargs):
    """
    Downloads a single HRRR file to a local directory. Kwargs
    allow downloads of both alaska files and forecast files
    by setting forecast_hour=hh or alaska=True.

    Inputs: Directory to save HRRR output files to, and year, month,
            day, and hour to download HRRR file for.

    Returns: Success code, time (in seconds) for function to run,
             path to file
    """

    start_time = datetime.now()

    if kwargs.get('alaska') == True:
        if kwargs.get('forecast_hour'):
            forecast_hour = kwargs.get('forecast_hour')

            url = "https://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr."+ \
               str(year)+str(month).zfill(2)+str(day).zfill(2)+ \
               "/alaska/hrrr.t"+str(hour).zfill(2)+"z.wrfprsf" + \
               str(forecast_hour).zfill(2)+".ak.grib2"''
        else:
            url = "https://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr."+ \
               str(year)+str(month).zfill(2)+str(day).zfill(2)+ \
               "/alaska/hrrr.t"+str(hour).zfill(2)+"z.wrfprsf00.ak.grib2"
    else:
        if kwargs.get('forecast_hour'):
            forecast_hour = kwargs.get('forecast_hour')

            url = "https://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr."+ \
               str(year)+str(month).zfill(2)+str(day).zfill(2)+ \
               "/conus/hrrr.t"+str(hour).zfill(2)+"z.wrfprsf"+ \
               str(forecast_hour).zfill(2)+".grib2"''
        else:
            url = "https://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr."+ \
               str(year)+str(month).zfill(2)+str(day).zfill(2)+ \
               "/conus/hrrr.t"+str(hour).zfill(2)+"z.wrfprsf00.grib2"''

    ssl._create_default_https_context = ssl._create_unverified_context

    file_name = "hrrr."+str(year)+str(month).zfill(2)+str(day).zfill(2)+"."+url.split('/')[-1].split("hrrr.",1)[1]
    dest_path = os.path.join(save_dir, file_name)

    try:
        print("starting download...")
        urllib.request.urlretrieve(url, dest_path) #Retrieve the file and write it as a grbfile
    except urllib.error.URLError as e:
        print(e.reason)

        elapsed_time = datetime.now() - start_time
        return 0, elapsed_time.total_seconds()

    elapsed_time = datetime.now() - start_time
    return 1, elapsed_time.total_seconds(), dest_path

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

def raob_csv_sounding_hrrr(file_path, save_path, sounding_lat=DEFAULT_LAT, sounding_lon=DEFAULT_LON, **kwargs):
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

    if kwargs.get('verbose') == True:
        verbose = True
        print("INFO: VERBOSE mode turned ON")
    else:
        verbose = False

    if kwargs.get('debug') == True:
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

    ds = xr.open_dataset(file_path, engine='cfgrib',
                        backend_kwargs={'filter_by_keys':{'typeOfLevel': 'isobaricInhPa'},'errors':'ignore'})

        
    date_time = ds.time.data #Grabbing the datetime object
    datetime_str = str(date_time) #Converting datetime object back to a string 
    date_str = datetime_str[0:10]
    time_str = datetime_str[11:19]
    date_time_str = date_str +" "+ time_str
        
    date = date_time_str

    # Becuase the grib reader reads longitude from 0-360 and not -180-180
    # we have to adjust the `sounding_lon`.
    if sounding_lon < 0 :
        sounding_lon += 360 

    abslat = np.abs(ds.latitude-sounding_lat)
    abslon = np.abs(ds.longitude-sounding_lon)

    c = np.maximum(abslon, abslat)

    ([idx_y], [idx_x]) = np.where(c == np.min(c))

    point_ds = ds.sel(y=idx_y, x=idx_x)

    print('Requested Point:', sounding_lat, sounding_lon)
    print('Nearest Model Point', point_ds.latitude.data, point_ds.longitude.data)

    latitude_float = round(float(point_ds.latitude.data),2) #Converts the array to a float and rounds it to stick into dataframe later
    longitude_float = round(float(point_ds.longitude.data - 360),2) #Same as for longitude
    longitude_float  = abs(longitude_float)

    press = point_ds.isobaricInhPa.data
    tmp = point_ds.t.data
    dpt = point_ds.dpt.data
    uwind = point_ds.u.data
    vwind = point_ds.v.data
    hgt = point_ds.gh.data
    
    #Convert Kelvin temps to C
    tmpc = tmp -273.15
    dptc = dpt -273.15
    
    #Convert u & v wind to wind speed
    #wspd = np.sqrt(np.square(uwind) + np.square(vwind))
    
    #Convert u & v winds (m/s) to kts
    uwindkts = np.dot(uwind, 1.94384)
    vwindkts = np.dot(vwind, 1.94384)

    dsur = xr.open_dataset(file_path,
                           engine='cfgrib',
                           backend_kwargs=dict(filter_by_keys={'stepType': 'instant', 'typeOfLevel': 'surface'}))
        
    point_dsur = dsur.sel(y=idx_y, x=idx_x)

    presssurface = point_dsur.sp.data
    hgtsurface = point_dsur.orog.data
    
    #Convert Pa Pressure to hpa
    presssurfacePa = presssurface * 0.01
    
    ####Redo Process for 2m Above Ground Level####
    
    #Open the grib2 file with xarray and cfgrib
    
    dg = xr.open_dataset(file_path, engine='cfgrib',
                          backend_kwargs={'filter_by_keys':{'typeOfLevel': 'heightAboveGround','level':2}})

    point_dg = dg.sel(y=idx_y, x=idx_x)
    
    # These are the profiles you want...
    tmp2m = point_dg.t2m.data
    dpt2m = point_dg.d2m.data
    
    tmp2mc = tmp2m -273.15
    dpt2mc = dpt2m -273.15
    pressPa2m = presssurfacePa - .2
    hgt2m = hgtsurface + 2.0
    
    d10m = xr.open_dataset(file_path, engine='cfgrib',
                          backend_kwargs={'filter_by_keys':{'typeOfLevel': 'heightAboveGround','level':10}})

    point_d10m = d10m.sel(y=idx_y, x=idx_x)
    
    # These are the profiles you want...
    uwind_10m = point_d10m.u10.data
    vwind_10m = point_d10m.v10.data
    
    #Convert u & v winds (m/s) to kts
    uwindkts_10m = np.dot(uwind_10m, 1.94384)
    vwindkts_10m = np.dot(vwind_10m, 1.94384)
    
    #Create and combine separate data frames in main data frame
    df = pd.DataFrame(data=[press, tmpc, dptc, uwindkts, vwindkts, hgt])
    #d1 = pd.DataFrame(data=[presssurfacePa,tmpsurfacec,-999,'','',hgtsurface.item(0)]) #Not using surface data becasue no dewpoint present at the surface
    d2 = pd.DataFrame(data=[pressPa2m,tmp2mc,dpt2mc,uwindkts_10m,vwindkts_10m,hgt2m])
    #d1 = d1.T
    d2 = d2.T
    #df2 = pd.concat([d1,d2],axis=0)
    df_t = df.T
    main_df= pd.concat([d2,df_t],axis=0,ignore_index=True)
    
    #Fill Nan arrays with -999 and get rid of Nan arrays for U & V Columns
    main_df[3].fillna('', inplace=True)
    main_df[4].fillna('', inplace=True)
    
    #Removes the pressure layers below the surface of the ground
    main_df = main_df[main_df[0] <= presssurfacePa]
        
    main_df = main_df.round(decimals=2)
    elev = round(float(hgtsurface)) #Rounding surface elevation for dataframe

    if kwargs.get('sounding_title'):
        csv_name = kwargs.get('sounding_title')
        file_name = date.replace(":","_").replace("-", "_") + "_" + csv_name + "_HRRR_RAOB.csv"
    else:
        csv_name = "UNNAMED SOUNDING"
        file_name = date.replace(":","_").replace("-", "_") + "_HRRR_RAOB.csv"
    
    d = {0:['RAOB/CSV','DTG','LAT','LON','ELEV','MOISTURE','WIND','GPM','MISSING','RAOB/DATA','PRES'],
         1:[csv_name,date,latitude_float,longitude_float,elev,'TD','kts','MSL',-999,'','TEMP'],2:['','','N','W','m','','U/V','','','','TD'],3:['','','','','','','','','','','UU'],
         4:['','','','','','','','','','','VV'],5:['','','','','','','','','','','GPM']}
    df_2 = pd.DataFrame(data=d)
    
    main_df = pd.concat([df_2,main_df],axis=0,ignore_index=True) #Combines the RAOB Header Format with the sounding data

    dest_path = os.path.join(save_path, file_name)

    main_df.to_csv(dest_path, index=False, header=False)

    if verbose:
        print("FILE: Saved File: " + file_name + " to " + save_path)

    elapsed_time = datetime.now() - start_time
    return 1, elapsed_time.total_seconds(), dest_path

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

    input_file_path = "/Users/rpurciel/Documents/Maritime Heli N311MH/HRRR (not ak)/"

    input_save_dir = input_file_path

    lat = 70.649215

    lon = -158.561911

    files = sorted(glob.glob(input_file_path + "*.grib2"))

    for file in files:

        print(file[-12:-9])

        raob_csv_sounding_hrrr(file, input_save_dir, lat, lon, sounding_title=file[-12:-9])








