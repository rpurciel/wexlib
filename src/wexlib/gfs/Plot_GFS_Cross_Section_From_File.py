#TODO: Lots of work on this baby right here
#   - Allow user defined variables
#   - Allow user defined tweaks
#   - Find which variables need to be:
#       A. converted into different units
#       B. indexed to make them not appear in a wierd wa¥
#   - Custom axis scaling

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 17 10:09:24 2023

@author: Ryan Purciel
"""

### Parameters START

start = (-48, -72.054124) 
end = (-90, -180)


### Parameters END

from herbie import FastHerbie
import pandas as pd
import numpy as np
import xarray as xr
import metpy as mp
import metpy.calc as mpcalc
import matplotlib.pyplot as plt
import math

def plot_cross_section_from_gfs(file_path, save_path, start_point, end_point, var_list):
    
    data = []
    for var in var_list:
        data += [xr.open_dataset(path, engine="cfgrib", backend_kwargs={'filter_by_keys' : {'typeOfLevel': 'isobaricInhPa', 'shortName' : var}})]

    data = xr.merge(data).metpy.parse_cf()
    
    timestr = str(pd.to_datetime(data['time'].values).strftime("%Y-%m-%d %H:%M"))
    timestr_file = str(pd.to_datetime(data['time'].values).strftime("%Y%m%d_%H"))
    fcsthrstr = str(data['step'].values.astype('timedelta64[h]')).replace(" hours", "")
    
    cross = mp.interpolate.cross_section(data, start, end).set_coords(('latitude', 'longitude'))
    #cross['pot'] = mpcalc.potential_temperature(cross['isobaricInhPa'],cross['t'])
    cross['u'] = cross['u'].metpy.convert_units('knots')
    cross['v'] = cross['v'].metpy.convert_units('knots')
    cross['w'], cross['n'] = mpcalc.cross_section_components(cross['u'], cross['v'])
    cross['wspd'] = mpcalc.wind_speed(cross['w'], cross['n'])
    
    fig = plt.figure(1, figsize=(16., 9.))
    ax = plt.axes()
    
    # Plot RH using contourf
    rh_contour = ax.contourf(cross['latitude'], cross['isobaricInhPa'], cross['wspd'],
                             levels=np.arange(0, int(math.ceil(cross['wspd'].max()/10))*10, 5), cmap='BuPu')
    rh_colorbar = fig.colorbar(rh_contour)
    
    
    # Plot winds using the axes interface directly, with some custom indexing to make the barbs
    # less crowded
    wind_slc_vert = range(0, len(cross['isobaricInhPa']), 3)
    wind_slc_horz = slice(5, 100, 5)
    ax.barbs(cross['latitude'][wind_slc_horz], cross['isobaricInhPa'][wind_slc_vert],
             cross['w'][wind_slc_vert, wind_slc_horz],
             cross['n'][wind_slc_vert, wind_slc_horz], color='k', flip_barb=True)
    
    # Adjust the y-axis to be logarithmic
    ax.set_yscale('symlog')
    ax.set_yticklabels(np.arange(1000, 50, -100))
    ax.set_ylim(cross['isobaricInhPa'].max(), cross['isobaricInhPa'].min())
    ax.set_yticks(np.arange(1000, 50, -100))
    
    ax.set_xlim(cross['latitude'].min(), cross['latitude'].max())


    ax.set_title('GFS Cross-Section, Surface to TOA\n', fontsize=14, fontweight='bold', loc='center')
    ax.set_title('Location: '+str(start)+ ' to ' + str(end) +'     Forecast Hr: ['+ fcsthrstr + "]     Valid: " + timestr,  loc='left')
    ax.set_ylabel('Pressure (hPa)')
    ax.set_xlabel('Latitude (degrees south)')
    rh_colorbar.set_label('Wind Speed (knots)')
    
    plttitle = timestr_file + "00Z_fcst" + fcsthrstr
    plt.savefig(plttitle + ".png", bbox_inches="tight", dpi=300)
    print("Saved File: " + plttitle + ".png")
    plt.close(fig)
    