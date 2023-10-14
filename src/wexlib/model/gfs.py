import sys
import os
import glob
from datetime import datetime
import ssl
import urllib.request

import xarray as xr
import numpy as np
import pandas as pd

def download_model(save_dir, year, month, day, hour, **kwargs):
	"""
    Downloads a single model file to a local directory. 

    Inputs: Directory to save model output files to, and year, month,
            day, and hour to download model file for.

    Returns: Success code, time (in seconds) for function to run,
             path to file
    """
	start_time = datetime.now()

	if kwargs.get('forecast_hour'):
        forecast_hour = kwargs.get('forecast_hour')

	    url = "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs."+ \
	    	str(year)+str(month).zfill(2)+str(day).zfill(2)+ \
	    	"/"+str(hour).zfill(2)+"/atmos/gfs.t"+str(hour).zfill(2)+\
	    	"z.pgrb2.0p25.f"+str(forecast_hour).zfill(3)
	    	file_name = "gfs."+str(hour).zfill(2)+"z.pgrb2.0p25.f"+ \
	    	str(forecast_hour).zfill(3)
    else:
    	url = "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs."+ \
	    	str(year)+str(month).zfill(2)+str(day).zfill(2)+ \
	    	"/"+str(hour).zfill(2)+"/atmos/gfs.t"+str(hour).zfill(2)+\
	    	"z.pgrb2.0p25.anl"''
	    	file_name = "gfs."+str(hour).zfill(2)+"z.pgrb2.0p25.anl"

	ssl._create_default_https_context = ssl._create_unverified_context

    dest_path = os.path.join(save_dir, file_name)

    try:
        urllib.request.urlretrieve(url, dest_path) #Retrieve the file and write it as a grbfile
    except urllib.error.URLError as e:
        print(e.reason)

        elapsed_time = datetime.now() - start_time
        return 0, elapsed_time.total_seconds()

	elapsed_time = datetime.now() - start_time
	return 1, elapsed_time.total_seconds(), dest_path

def plot_plan_view_model(file_path, save_dir, level, variables, points, **kwargs):
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

	elapsed_time = datetime.now() - start_time
	return 1, elapsed_time.total_seconds(), dest_path

def plot_cross_section_model(file_path, save_dir, start_point, end_point, variables, points, top_level, **kwargs):
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

def raob_csv_sounding_model(file_path, save_path, sounding_lat=DEFAULT_LAT, sounding_lon=DEFAULT_LON, **kwargs):
    """
	Using a lat/lon, generates a CSV sounding for use in RAOB.

    Latitude and longitude are specified as decimal floats, with
    the default value set in this file (if none are specified).

    Returns: Success code, time (in seconds) for function to run,
             path to output file
    """

    start_time = datetime.now()

    name = "GFS-generated Sounding"

    ds = xr.open_dataset(file_path, engine='cfgrib',
                        backend_kwargs={'filter_by_keys':{'typeOfLevel': 'isobaricInhPa'},'errors':'ignore'})

    timestr = str(pd.to_datetime(ds['time'].values).strftime("%Y-%m-%d %H:%M:00"))
    timestr_file = str(pd.to_datetime(ds['time'].values).strftime("%Y%m%d_%H"))
    fcsthrstr = str(ds['step'].values.astype('timedelta64[h]')).replace(" hours", "")

    # Becuase the grib reader reads longitude from 0-360 and not -180-180
    # we have to adjust the `sounding_lon`.
    if sounding_lon < 0 :
        sounding_lon += 360 

    point_ds = ds.sel(longitude=sounding_lon, latitude=sounding_lat, method='nearest')

    print('Requested Point:', sounding_lat, sounding_lon)
    print('Nearest Model Point', point_ds.latitude.data, point_ds.longitude.data)

    latitude_float = round(float(point_ds.latitude.data),2) #Converts the array to a float and rounds it to stick into dataframe later
    longitude_float = round(float(point_ds.longitude.data - 360),2) #Same as for longitude

    press = point_ds.isobaricInhPa.data
    tmp = point_ds.t.data
    hgt = point_ds.gh.data 
    
    #Convert Kelvin temps to C
    tmpc = tmp -273.15

    dsw = xr.open_dataset(file_path, engine='cfgrib',
                        backend_kwargs={'filter_by_keys':{'typeOfLevel': 'isobaricInhPa', 'units': 'm s**-1'},'errors':'ignore'})

    point_dsw = dsw.sel(longitude=sounding_lon, latitude=sounding_lat, method= 'nearest')

    uwind = point_dsw.u.data
    vwind = point_dsw.v.data
    
    #Convert u & v winds (m/s) to kts
    uwindkts = np.dot(uwind, 1.94384)
    vwindkts = np.dot(vwind, 1.94384)

    dsrh = xr.open_dataset(file_path, engine='cfgrib',
                        backend_kwargs={'filter_by_keys':{'typeOfLevel': 'isobaricInhPa', 'shortName': 'r'}})

    point_dsrh = dsrh.sel(longitude=sounding_lon, latitude=sounding_lat, method= 'nearest')

    relativehumidity = point_dsrh.r.data
    
    rhappend = relativehumidity
    rhappend = np.append(relativehumidity,0)
    rhappend = np.append(rhappend,0)
    
    #Convert RH to Dewpoint Temp
    A = 17.27
    B = 237.7
    
    #dwptc = B * (np.log(rhappend/100.) + (A*tmpc/(B+tmpc))) / (A-np.log(rhappend/100.)-((A*tmpc)/(B+tmpc)))
    dwptc = B * (np.log(relativehumidity/100.) + (A*tmpc/(B+tmpc))) / (A-np.log(relativehumidity/100.)-((A*tmpc)/(B+tmpc)))

    dsur = xr.open_dataset(file_path, engine='cfgrib',
                    backend_kwargs={'filter_by_keys':{'typeOfLevel': 'surface', 'stepType' : 'instant'},'errors':'ignore'})

    point_dsur = dsur.sel(longitude=sounding_lon, latitude=sounding_lat, method= 'nearest')

    presssurface = point_dsur.sp.data
    tmpsurface = point_dsur.t.data
    hgtsurface = point_dsur.orog.data
    
    #Convert Kelvin temps to C
    tmpsurfacec = tmpsurface -273.15
    
    #Convert Pa Pressure to hpa
    presssurfacePa = presssurface * 0.01
    
    ####Redo Process for 2m Above Ground Level####
    
    #Open the grib2 file with xarray and cfgrib
    
    dg = xr.open_dataset(file_path, engine='cfgrib',
                          backend_kwargs={'filter_by_keys':{'typeOfLevel': 'heightAboveGround','level':2}})

    point_dg = dg.sel(longitude=sounding_lon, latitude=sounding_lat, method= 'nearest')
    
    # These are the profiles you want...
    tmp2m = point_dg.t2m.data
    #d2m = point_dg.d2m.data
    rh2m = point_dg.r2.data
    
    #Convert Kelvin temps to C
    tmp2mc = tmp2m -273.15
    #d2mc = d2m -273.15
    
    d2mc = B * (np.log(rh2m/100.) + (A*tmp2mc/(B+tmp2mc))) / (A-np.log(rh2m/100.)-((A*tmp2mc)/(B+tmp2mc)))
    
    press2mPa = presssurfacePa - .2
    hgt2m = hgtsurface + 2.0
    
    d10m = xr.open_dataset(file_path, engine='cfgrib',
                          backend_kwargs={'filter_by_keys':{'typeOfLevel': 'heightAboveGround','level':10}})

    point_d10m = d10m.sel(longitude=sounding_lon, latitude=sounding_lat, method= 'nearest')
    
    print('requested:', sounding_lat, sounding_lon)
    print('  nearest:', point_d10m.latitude.data, point_d10m.longitude.data)
    
    # These are the profiles you want...
    uwind_10m = point_d10m.u10.data
    vwind_10m = point_d10m.v10.data
    
    #Convert u & v winds (m/s) to kts
    uwindkts_10m = np.dot(uwind_10m, 1.94384)
    vwindkts_10m = np.dot(vwind_10m, 1.94384)
    
    #Create and combine separate data frames in main data frame
    df = pd.DataFrame(data=[press, tmpc, dwptc, uwindkts, vwindkts, hgt])
    #d1 = pd.DataFrame(data=[presssurfacePa,tmpsurfacec,-999,'','',hgtsurface.item(0)]) #Not using surface data becasue no dewpoint present at the surface
    d2 = pd.DataFrame(data=[press2mPa,tmp2mc,d2mc.item(0),uwindkts_10m,vwindkts_10m,hgt2m])
    #d1 = d1.T
    d2 = d2.T
    #df2 = pd.concat([d1,d2],axis=0)
    df_t = df.T
    main_df= pd.concat([d2,df_t],axis=0,ignore_index=True)
    
    #Fill Nan arrays with -999 and get rid of Nan arrays for U & V Columns
    main_df[2].fillna(-999, inplace=True)
    main_df[3].fillna('', inplace=True)
    main_df[4].fillna('', inplace=True)
    
    #Removes the pressure layers below the surface of the ground
    main_df = main_df[main_df[0] <= presssurfacePa]
        
    main_df = main_df.round(decimals=2)
    elev = round(float(hgtsurface)) #Rounding surface elevation for dataframe
    
    d = {0:['RAOB/CSV','DTG','sounding_lat','sounding_lon','ELEV','MOISTURE','WIND','GPM','MISSING','RAOB/DATA','PRES'],
         1:[name,timestr,latitude_float,longitude_float,elev,'TD','kts','MSL',-999,'','TEMP'],2:['','','N','W','m','','U/V','','','','TD'],3:['','','','','','','','','','','UU'],
         4:['','','','','','','','','','','VV'],5:['','','','','','','','','','','GPM']}
    df_2 = pd.DataFrame(data=d)
    
    main_df = pd.concat([df_2,main_df],axis=0,ignore_index=True) #Combines the RAOB Header Format with the sounding data

    file_name = timestr_file + "00Z_fcst" + fcsthrstr + "_RAOB.csv"
    dest_path = os.path.join(save_path, file_name)

    main_df.to_csv(dest_path, index=False, header=False)

    print("Saved File: " + file_name + " to " + save_path)

	elapsed_time = datetime.now() - start_time
	return 1, elapsed_time.total_seconds(), dest_path

def _calculate_variable_model(variable_short_name, input_data):
	"""Internal function: given a short name, return a calculated variable using input data.

	This works strictly with defined calculations, and only with standard (as defined
	for this library) units.
	"""

def _convert_natural_name_to_short_name_model(natural_name):
	"""Internal function: given a natural variable name, return a short name that works for the specified model.

	This works with certain natural names, and only for defined variables.
	"""




