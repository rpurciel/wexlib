#TODO: Refactor old code to make it human readable

import numpy as np
import xarray as xr
import pandas as pd
import time
import os

#wxextreme office
DEFAULT_LAT = 39.446030
DEFAULT_LON = -119.771627

def generate_raobcsv_sounding_from_gfs(file_path, save_path, sounding_lat=DEFAULT_LAT, sounding_lon=DEFAULT_LON):

    if sounding_lat == DEFAULT_LAT and sounding_lon == DEFAULT_LON:
       print("Warning: No sounding point selected, continuing with default coordinates. "
            "\nCould have unintended behavior!!")

    name = "GFS"

    date = "2023-01-01 00:00:00"

    ds = xr.open_dataset(file_path, engine='cfgrib',
                        backend_kwargs={'filter_by_keys':{'typeOfLevel': 'isobaricInhPa'},'errors':'ignore'})

    timestr = str(pd.to_datetime(ds['time'].values).strftime("%Y-%m-%d %H:%M"))
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
         1:[name,date,latitude_float,longitude_float,elev,'TD','kts','MSL',-999,'','TEMP'],2:['','','N','W','m','','U/V','','','','TD'],3:['','','','','','','','','','','UU'],
         4:['','','','','','','','','','','VV'],5:['','','','','','','','','','','GPM']}
    df_2 = pd.DataFrame(data=d)
    
    main_df = pd.concat([df_2,main_df],axis=0,ignore_index=True) #Combines the RAOB Header Format with the sounding data

    file_name = timestr_file + "00Z_fcst" + fcsthrstr + "_RAOB.csv"

    main_df.to_csv(os.path.join(save_path, file_name), index=False, header=False)

    print("Saved File: " + file_name + " to " + save_path)

    return 1

if __name__ == "__main__":

    print("Starting script...")

    while True:
        gfs_path = input("Input path to GFS File: ")
        gfs_path = "/Users/rpurciel/Documents/Minden Soundings/gfs.t18z.pgrb2.0p25.f003"

        if not os.path.isfile(gfs_path):
            print("ERROR: Path to GFS file is not valid or file does not exist.")
            continue

        save_dir_path = input("Input path to save directory: ")
        save_dir_path = "/Users/rpurciel/Documents/Minden Soundings"
        # print(save_dir_path[-1:])
        # if save_dir_path[-1:] != "/":
        #     save_dir_path += "/"

        if not os.path.isdir(save_dir_path):
            print("ERROR: Save directory is not valid or does not exist.")
            continue

        input_lat = input("Input sounding latitude point: ")
        input_lat = 39.000049
        input_lon = input("Input sounding longitude point: ")
        input_lon = -119.752236

        result = generate_raobcsv_sounding_from_gfs(gfs_path, save_dir_path, input_lat, input_lon)

        if result == 1:
            input("Press any key to exit...")
            break






