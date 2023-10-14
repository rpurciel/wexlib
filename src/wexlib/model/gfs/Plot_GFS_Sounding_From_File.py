import pandas as pd
import numpy
import xarray as xr
import time
import cfgrib
import metpy
from metpy.units import units
import metpy.calc as mpcalc
import matplotlib.pyplot as plt
from metpy.plots import SkewT
import os

#wxextreme office
DEFAULT_LAT = 39.446030
DEFAULT_LON = -119.771627

def plot_sounding_from_gfs(file_path, save_path, sounding_lat=DEFAULT_LAT, sounding_lon=DEFAULT_LON):

	if sounding_lat == DEFAULT_LAT and sounding_lon == DEFAULT_LON:
		print("Warning: No sounding point selected, continuing with default coordinates. "
			"\nCould have unintended behavior!!")

	if sounding_lon < 0: #readjust lon for grib reader to read the correct coordinate
		sounding_lon += 360 

	data = []
	for var in ['t', 'u', 'v', 'r']:
		data += [xr.open_dataset(file_path, engine="cfgrib", backend_kwargs={'filter_by_keys' : {'typeOfLevel': 'isobaricInhPa', 'shortName' : var}})]

	data = xr.merge(data)
	data = data.sel(latitude=sounding_lat, longitude=sounding_lon, method='nearest')

	timestr = str(pd.to_datetime(data['time'].values).strftime("%Y-%m-%d %H:%M"))
	timestr_file = str(pd.to_datetime(data['time'].values).strftime("%Y%m%d_%H"))
	fcsthrstr = str(data['step'].values.astype('timedelta64[h]')).replace(" hours", "")

	print("\nSelected GFS run at " + timestr)
	print("Forecast hour: " + fcsthrstr)
	
	temp = data.t.data * units("K")
	T = temp.to(units("degC"))

	rh = data.r.data * units("%")
	p = data.isobaricInhPa.data * units("hPa")

	u = data.u.data * units("m/s")
	v = data.v.data * units("m/s")

	Td = mpcalc.dewpoint_from_relative_humidity(T, rh)

	fig = plt.figure(figsize=(9, 9))
	skew = SkewT(fig, rotation=45)

	# Plot the data using normal plotting functions, in this case using
	# log scaling in Y, as dictated by the typical meteorological plot.
	skew.plot(p, T, 'r')
	skew.plot(p, Td, 'g')
	skew.plot_barbs(p, u, v, xloc=1.06, plot_units=units("knots"))
	skew.ax.set_ylim(1000, 100)
	#skew.ax.set_xlim(-40, 60)

	#dynamic x limit based on temperature at surface
	skew.ax.set_xlim(T[0].magnitude - 40, T[0].magnitude + 40)

	# Set some better labels than the default
	skew.ax.set_xlabel(f'Temperature ({T.units:~P})')
	skew.ax.set_ylabel(f'Pressure ({p.units:~P})')

	# Calculate LCL height and plot as black dot. Because `p`'s first value is
	# ~1000 mb and its last value is ~250 mb, the `0` index is selected for
	# `p`, `T`, and `Td` to lift the parcel from the surface. If `p` was inverted,
	# i.e. start from low value, 250 mb, to a high value, 1000 mb, the `-1` index
	# should be selected.
	# lcl_pressure, lcl_temperature = mpcalc.lcl(p[0], T[0], Td[0])
	# skew.plot(lcl_pressure, lcl_temperature, 'ko', markerfacecolor='black')

	# # Calculate full parcel profile and add to plot as black line
	# prof = mpcalc.parcel_profile(p, T[0], Td[0]).to('degC')
	# skew.plot(p, prof, 'k', linewidth=2)

	# Shade areas of CAPE and CIN
	# skew.shade_cin(p, T, prof, Td)
	# skew.shade_cape(p, T, prof)

	# Add the relevant special lines
	skew.plot_dry_adiabats()
	skew.plot_moist_adiabats()
	skew.plot_mixing_lines()

	sounding_point = (sounding_lat, sounding_lon - 360)

	skew.ax.set_title('GFS Model Sounding, Surface to 100 mb\n', fontsize=14, fontweight='bold', loc='center')
	skew.ax.set_title('Location: '+str(sounding_point)+ '     Forecast Hr: ['+ fcsthrstr + "]     Valid: " + timestr,  loc='left')

	# Show the plotprint("Saved File: " + plttitle + ".png")
	#plttitle = timestr_file + "00Z_fcst" + fcsthrstr + "_" + str(sounding_lat)[:4].replace(".","_") + "_" str(sounding_lon)[:4].replace(".","_") + ".png"
	plttitle = timestr_file + "00Z_fcst" + fcsthrstr + ".png"

	plt.savefig(os.path.join(save_path, plttitle), bbox_inches="tight", dpi=300)
	print("Saved File: " + plttitle + " to " + save_path)
	plt.close()

	return 1

if __name__ == "__main__":

	while True:
		gfs_path = input("Input path to GFS File: ")

		if not os.path.isfile(gfs_path):
			print("ERROR: Path to GFS file is not valid or file does not exist.")
			continue

		save_dir_path = input("Input path to save directory: ")

		if not os.path.isdir(save_dir_path):
			print("ERROR: Save directory is not valid or does not exist.")
			continue

		input_lat = input("Input sounding latitude point: ")
		input_lon = input("Input sounding longitude point: ")

		result = plot_sounding_from_gfs(gfs_path, save_dir_path, input_lat, input_lon)

		if result == 1:
			input("Press any key to exit...")
			break







