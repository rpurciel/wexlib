import sys
import os
import glob
from datetime import datetime
import re
import glob

import xarray as xr
import numpy as np
import metpy
import matplotlib
from matplotlib import cm
import matplotlib.pyplot as plt
import cartopy 
import cartopy.crs as crs
import cartopy.crs as ccrs
import cartopy.feature as cfeat
import cartopy.io.shapereader as shpreader
from cartopy.feature import NaturalEarthFeature
import s3fs

#from ..processing_util import calc

#global params
DEF_BBOX = [-126.3, -65.2, 24.1, 50.8] #CONUS - WESN

DEF_CORRECT_CLIP = True
DEF_CORRECT_GAMMA = True
DEF_GAMMA = 2.2

DEF_GEOG_VISIBLE = True
DEF_GEOG_DRAW_STATES = True
DEF_GEOG_DRAW_COASTLINES = True

DEF_COLORBAR_VISIBLE = False
DEF_COLORBAR_LABEL = 'Pixel Brightness'

DEF_POINT_COLOR = 'black'
DEF_POINT_SIZE = 8
DEF_POINT_MARKER = 'o'
DEF_POINT_LABEL_VISIBLE = True
DEF_POINT_LABEL_COLOR = 'black'
DEF_POINT_LABEL_FONTSIZE = 10
DEF_POINT_LABEL_XOFFSET = -0.85
DEF_POINT_LABEL_YOFFSET = 0

DEF_FILE_DPI = 300

#download params
DEF_SECTOR = 'C' #CONUS

#single-band params
DEF_SB_PALLETE = 'greys_r'

#multi-band params
DEF_MB_PALLETE = 'terrain'

def aws_readbucket_goes(year, julian_day, hour, satellite, sector=DEF_SECTOR, **kwargs):
	"""
	Downloads a single model file to a local directory. 

	Inputs: Directory to save model output files to, and year, month,
			day, and hour to download model file for.

	Returns: Success code, time (in seconds) for function to run,
			 path to file
	"""

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

	try:
		aws = s3fs.S3FileSystem(anon=True)
	except Exception as e:
		error_str = ("Error: " + e)

		if verbose:
			print(error_str)

		return 0, error_str

	hour = str(hour).zfill(2)

	julian_day = str(julian_day).zfill(3)

	try:
		list_of_aws_urls = np.array(aws.ls(f'noaa-{satellite}/ABI-L2-MCMIP{sector}/{year}/{julian_day}/{hour}'))
	except Exception as e:
		error_str = ("ERROR: ", e)

		if verbose:
			print("ERROR:", e)

		return 0, error_str

	if verbose:
		print(f"INFO: Total = {len(list_of_aws_urls)} files")
		print(f'''INFO: Parameters:\nINFO: Year = {year}\nINFO: Julian Day = {julian_day}\n'''
			  f'''INFO: Hour = {hour}\nINFO: Satellite = {satellite}\nINFO: Sector = {sector}''')
	if debug:
		print("DEBUG: Files to download:")
		print(list_of_aws_urls)

	return 1, list_of_aws_urls

def download_singlefile_aws_goes(save_dir, aws_url, **kwargs):
	"""
	Downloads a single model file to a local directory. 

	Inputs: Directory to save model output files to, and year, month,
			day, and hour to download model file for.

	Returns: Success code, time (in seconds) for function to run,
			 path to file
	"""
	start_time = datetime.now()

	if not os.path.exists(save_dir):
		os.makedirs(save_dir)

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

	try:
		aws = s3fs.S3FileSystem(anon=True)
	except Exception as e:
		error_str = ("ERROR: " + e)

		if verbose:
			print(error_str)

		elapsed_time = datetime.now() - start_time
		return 0, elapsed_time.total_seconds(), error_str

	file_name = aws_url.split('/')[-1]
	dest_path = os.path.join(save_dir, file_name)

	if debug:
		print(f"INFO: Starting to download {file}")

	try:
		aws.get(aws_url, os.path.join(save_dir, file_name))
	except Exception as e:
		error_str = ("ERROR: " + e)

		if verbose:
			print(error_str)

		elapsed_time = datetime.now() - start_time
		return 0, elapsed_time.total_seconds(), error_str

	if verbose:
		print(f'INFO: Finished downloading file {file_name}')

	elapsed_time = datetime.now() - start_time
	return 1, elapsed_time.total_seconds(), dest_path

def plot_single_band_goes(file_path, save_dir, band, points, pallete=DEF_SB_PALLETE, bbox=DEF_BBOX, **kwargs):
	"""
	Using a bounding box, plots a single satellite band and any points.

	Band is specified as a int between 1-16, corresponding to GOES
	band id.

	Bounding box is specified via a list of length 4:
	[ll corner lat, ll corner lon, ur corner lat, ur corner lon]

	Points are specified as a list of tuples, including label:
	[(lat1, lon1, label1), (lat2, lon2, label2), ...]

	Pallete is specified as a matplotlib compatable color pallete
	name string. Suffix '_r' to reverse the color pallete

	Returns: Success code, time (in seconds) for function to run,
			 path to file
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

	try:
		data = xr.open_dataset(file_path, engine="netcdf4")
		if verbose:
			print("INFO: File at " + file_path + " opened successfully")
	except Exception as e:
		error_str = "Exception when opening file: " + e
		if verbose:
			print("INFO: " + error_str)

		elapsed_time = datetime.now() - start_time
		return 0, elapsed_time.total_seconds(), error_str

	scan_start = datetime.strptime(data.time_coverage_start, '%Y-%m-%dT%H:%M:%S.%fZ')
	scan_end = datetime.strptime(data.time_coverage_end, '%Y-%m-%dT%H:%M:%S.%fZ')
	file_created = datetime.strptime(data.date_created, '%Y-%m-%dT%H:%M:%S.%fZ')
	orbital_slot = data.orbital_slot #GOES-East, GOES-West, GOES-Test, etc.
	sat_id = data.platform_ID #G18, G17, G16, etc.
	if verbose:
		print(f"""INFO: Scan start: {scan_start}\nINFO: Scan end: {scan_end}\nINFO: File created: {file_created}"""
			  f"""\nINFO: Orbital slot: {orbital_slot}\nINFO: Satellite ID: {sat_id}""")
	

	#BAND SELECTION

	if band < 1 or band > 16:
		error_str = "Error: Selected band must be between 1-16 (Selected band: " + str(band) + "?)"
		if verbose:
			print("INFO: " + error_str)

		elapsed_time = datetime.now() - start_time
		return 0, elapsed_time.total_seconds(), error_str

	sel_band_str = 'CMI_C' + str(band).zfill(2)
	sel_band = data[sel_band_str].data
	
	if verbose:
		print("INFO: Selected band: ", sel_band_str)

	if debug:
		print("DEBUG: Band data (no processing): \n")
		print(sel_band)

	data = data.metpy.parse_cf('CMI_C02')
	geog_data = data.metpy.cartopy_crs
	x = data.x
	y = data.y

 	#DATA CORRECTIONS

	correct_clip = DEF_CORRECT_CLIP
	correct_gamma = DEF_CORRECT_GAMMA
	for arg, value in kwargs.items():
		if arg == "correct_clip":
			correct_clip = value
		if arg == "correct_gamma":
			correct_gamma = value
		if arg == "gamma":
			gamma = float(value)

	if correct_clip:
		sel_band = np.clip(sel_band, 0, 1)
		if verbose:
			print("CORRECT: Clipping correction applied.")
		if debug:
			print("Band data (post-clip): \n")
			print(sel_band)
	else:
		if verbose:
			print("CORRECT: No clipping correction applied.")
	
	if correct_gamma:
		gamma = DEF_GAMMA
		sel_band = np.power(sel_band, 1/gamma)
		if verbose:
			print("Gamma correction applied. Gamma factor: ", gamma)
		if debug:
			print("CORRECT: Band data (post-gamma correction): \n")
			print(sel_band)
	else:
		if verbose:
			print("CORRECT: No gamma correction applied.")

	fig = plt.figure(figsize=(16., 9.))
	ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
	
	ax.set_extent(bounds, crs=ccrs.PlateCarree())
	
	plt.imshow(sel_band, origin='upper',
		  extent=(x.min(), x.max(), y.min(), y.max()),
		  transform=geog_data,
		  interpolation='none',
		  cmap=cm.get_cmap(pallete))
	
	#COLORBAR

	colorbar_visible = DEF_COLORBAR_VISIBLE
	colorbar_label = DEF_COLORBAR_LABEL
	for arg, value in kwargs.items():
		if arg.startswith('colorbar_'):
			exec(f"{arg} = {value}") #dynamically set variables based on what was passed

	if colorbar_visible:
		plt.colorbar(ax=ax, orientation = "horizontal", pad=.05).set_label(colorbar_label)

		if verbose:
			print(f"COLORBAR: Drawing colorbar turned ON\nCOLORBAR: Label = {colorbar_label}")
	
	#GEOGRAPHY DRAWING

	geog_visible = DEF_GEOG_VISIBLE
	geog_draw_states = DEF_GEOG_DRAW_STATES
	geog_draw_coastlines = DEF_GEOG_DRAW_COASTLINES
	for arg, value in kwargs.items():
		if arg.startswith('geog_'):
			exec(f"{arg} = {value}") #dynamically set variables based on what was passed

	#TODO: Add in more geography drawing options
	if geog_visible:
		if geog_draw_states:
			ax.add_feature(ccrs.cartopy.feature.STATES)
			if verbose: 
				print("GEOG: Drawing states")
		if geog_draw_coastlines:
			ax.coastlines(resolution='50m', color='black', linewidth=1)
			if verbose:
				print("GEOG: Drawing coastlines")
	else:
		if verbose:
			print("GEOG: Geography drawing turned OFF")

	#POINT DRAWING

	point_color = DEF_POINT_COLOR
	point_size = DEF_POINT_SIZE
	point_marker = DEF_POINT_MARKER
	point_label_visible = DEF_POINT_LABEL_VISIBLE
	point_label_color = DEF_POINT_LABEL_COLOR
	point_label_fontsize = DEF_POINT_LABEL_FONTSIZE
	point_label_xoffset = DEF_POINT_LABEL_XOFFSET
	point_label_yoffset = DEF_POINT_LABEL_YOFFSET
	for arg, value in kwargs.items():
		if arg == "point_color":
			point_color = value
		if arg == "point_size":
			point_size = float(value)
		if arg == "point_marker":
			point_marker = value
		if arg == "point_label_visible":
			point_label_visible = value
		if arg == "point_label_color":
			point_label_color = value
		if arg == "point_label_fontsize":
			point_label_fontsize = float(value)
		if arg == "point_label_xoffset":
			point_label_xoffset = float(value)
		if arg == "point_label_yoffset":
			point_label_yoffset = float(value)

	if points:
		num_points = len(points)
		if verbose:
			print(f'''POINT: {num_points} points passed to be plotted\nPOINT:Formating Options:\nPOINT: Point color: {point_color}'''
				f'''\nPOINT: Point size: {point_size}\nPOINT: Point marker: {point_marker}\nPOINT: Label visibility: {point_label_visible}'''
				f'''\nPOINT: Label color: {point_label_color}\nPOINT: Label font size: {point_label_fontsize}'''
				f'''\nPOINT: Label x-offset: {point_label_xoffset}\nPOINT: Label y-offset: {point_label_yoffset}''')

		for point in points:
			x_axis = point[0]
			y_axis = point[1]
			label = point[2]
			
			ax.plot([y_axis],[x_axis], 
				  color=point_color, marker=point_marker)
		   
			if point_label_visible:
				ax.annotate(label, (y_axis + point_label_yoffset, x_axis + point_label_xoffset),
					  horizontalalignment='center', color=point_label_color, fontsize=point_label_fontsize,
					  transform=crs.PlateCarree(), annotation_clip=True, zorder=30)
		
	sel_band_name = data['band_id_C' + band.zfill(2)].long_name
	
	if kwargs.get('plot_title'):
		plt_title = kwargs.get('plot_title')
		if verbose:
			print("FILE: Plot title set manually to '" + plt_title + "'")
	else:
		plt_title = orbital_slot.replace("-Test", "") + " (" + sat_id.replace("G", "GOES-") + ") - " + human_product_name #ex: GOES-WEST (G16) - Day Cloud Phase
		if verbose:
			print("FILE: Plot title generated dynamically")
	
	plt.title(plt_title, loc='left', fontweight='bold', fontsize=15)
	plt.title('{}'.format(scan_end.strftime('%d %B %Y %H:%M UTC ')), loc='right')
	
	file_name = sat_id + "_" + sel_band_str + "_" + scan_end.strftime('%Y%m%d_%H%M%S%Z')
	
	if not os.path.exists(save_dir):
		os.makedirs(save_dir)

	dest_path = os.path.join(save_dir, file_name + ".png")
	
	file_dpi = DEF_FILE_DPI
	for arg, value in kwargs.items():
		if arg == "file_dpi":
			file_dpi = int(arg)

	plt.savefig(dest_path, bbox_inches="tight", dpi=file_dpi)
	if verbose:
		print("FILE: File titled " + file_name + " saved to " + save_dir)
		print("FILE: DPI: " + file_dpi)
		print("FILE: Full path: " + dest_path)
	plt.close()

	elapsed_time = datetime.now() - start_time
	return 1, elapsed_time.total_seconds(), dest_path

def plot_composite_goes(file_path, save_dir, product, points, bbox=DEF_BBOX, **kwargs):
	"""
	Using a bounding box, plots a single satellite band and any points.

	Band is specified as a int between 1-16, corresponding to GOES
	band id.

	Bounding box is specified via a list of length 4:
	[ll corner lat, ll corner lon, ur corner lat, ur corner lon]

	Points are specified as a list of tuples, including label:
	[(lat1, lon1, label1), (lat2, lon2, label2), ...]

	Pallete is specified as a matplotlib compatable color pallete
	name string. Suffix '_r' to reverse the color pallete

	Returns: Success code, time (in seconds) for function to run,
			 path to file
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

	try:
		data = xr.open_dataset(file_path, engine="netcdf4")
		if verbose:
			print("INFO: File at " + file_path + " opened successfully")
	except Exception as e:
		error_str = "Exception when opening file: " + e
		if verbose:
			print("INFO: " + error_str)

		elapsed_time = datetime.now() - start_time
		return 0, elapsed_time.total_seconds(), error_str

	scan_start = datetime.strptime(data.time_coverage_start, '%Y-%m-%dT%H:%M:%S.%fZ')
	scan_end = datetime.strptime(data.time_coverage_end, '%Y-%m-%dT%H:%M:%S.%fZ')
	file_created = datetime.strptime(data.date_created, '%Y-%m-%dT%H:%M:%S.%fZ')
	orbital_slot = data.orbital_slot #GOES-East, GOES-West, GOES-Test, etc.
	sat_id = data.platform_ID #G18, G17, G16, etc.
	if verbose:
		print(f"""INFO: Scan start: {scan_start}\nINFO: Scan end: {scan_end}\nINFO: File created: {file_created}"""
			  f"""\nINFO: Orbital slot: {orbital_slot}\nINFO: Satellite ID: {sat_id}""")


	#PRODUCT SELECTION

	red, green, blue, pallete, human_product_name = _calculate_composite_product_data(data, product)
	
	if verbose:
		print("INFO: Selected product: " + product)
		print("INFO: Auto-selected pallete: ", pallete)

	if debug:
		print("DEBUG: Band data (no processing)\nDEBUG: Red: \n")
		print(red)
		print("DEBUG: Green: ")
		print(green)
		print("DEBUG: Blue: ")
		print(blue)

	data = data.metpy.parse_cf('CMI_C02')
	geog_data = data.metpy.cartopy_crs
	x = data.x
	y = data.y
	

 	#DATA CORRECTIONS

	correct_clip = DEF_CORRECT_CLIP
	correct_gamma = DEF_CORRECT_GAMMA
	for arg, value in kwargs.items():
		if arg == "correct_clip":
			correct_clip = value
		if arg == "correct_gamma":
			correct_gamma = value
		if arg == "gamma":
			gamma = float(value)

	if correct_clip:
		red = np.clip(red, 0, 1)
		green = np.clip(green, 0, 1)
		blue = np.clip(blue, 0, 1)

		if verbose:
			print("CORRECT: Clipping correction applied.")
		if debug:
			print("DEBUG: Band data (post-clip)\nDEBUG: Red: \n")
			print(red)
			print("DEBUG: Green: ")
			print(green)
			print("DEBUG: Blue: ")
			print(blue)
	else:
		if verbose:
			print("CORRECT: No clipping correction applied.")
	
	if correct_gamma:
		gamma = DEF_GAMMA

		red = np.power(red, 1/gamma)
		green = np.power(green, 1/gamma)
		blue = np.power(blue, 1/gamma)

		if verbose:
			print("CORRECT: Gamma correction applied. Gamma factor: ", gamma)
		if debug:
			print("DEBUG: Band data (post-gamma correction)\nDEBUG: Red: \n")
			print(red)
			print("DEBUG: Green: ")
			print(green)
			print("DEBUG: Blue: ")
			print(blue)
	else:
		if verbose:
			print("CORRECT: No gamma correction applied.")

	rgb_composite = np.stack([red, green, blue], axis=2)
	
	fig = plt.figure(figsize=(16., 9.))
	ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
	
	ax.set_extent(bbox, crs=ccrs.PlateCarree())
	
	plt.imshow(rgb_composite, origin='upper',
		  extent=(x.min(), x.max(), y.min(), y.max()),
		  transform=geog_data,
		  interpolation='none')
	
	#COLORBAR

	colorbar_visible = DEF_COLORBAR_VISIBLE
	colorbar_label = DEF_COLORBAR_LABEL

	for arg, value in kwargs.items():
		if arg == 'colorbar_visible':
			colorbar_visible = value
		if arg == 'colorbar_label':
			colorbar_label = value

	if colorbar_visible:
		plt.colorbar(ax=ax, orientation = "horizontal", pad=.05).set_label(colorbar_label)
		if verbose:
			print(f"COLORBAR: Drawing colorbar turned ON\nCOLORBAR: Label = '{colorbar_label}'")
	
	#GEOGRAPHY DRAWING

	geog_visible = DEF_GEOG_VISIBLE
	geog_draw_states = DEF_GEOG_DRAW_STATES
	geog_draw_coastlines = DEF_GEOG_DRAW_COASTLINES
	for arg, value in kwargs.items():
		if arg == "geog_visible":
			geog_visible = value
		if arg == "geog_draw_states":
			geog_draw_states = value
		if arg == "geog_draw_coastlines":
			geog_draw_coastlines = value
	#TODO: Add in more geography drawing options


	if geog_visible:
		if geog_draw_states:
			ax.add_feature(ccrs.cartopy.feature.STATES)
			if verbose: 
				print("GEOG: Drawing states")
		if geog_draw_coastlines:
			ax.coastlines(resolution='50m', color='black', linewidth=1)
			if verbose:
				print("GEOG: Drawing coastlines")
	else:
		if verbose:
			print("GEOG: Geography drawing turned OFF")

	#POINT DRAWING

	point_color = DEF_POINT_COLOR
	point_size = DEF_POINT_SIZE
	point_marker = DEF_POINT_MARKER
	point_label_visible = DEF_POINT_LABEL_VISIBLE
	point_label_color = DEF_POINT_LABEL_COLOR
	point_label_fontsize = DEF_POINT_LABEL_FONTSIZE
	point_label_xoffset = DEF_POINT_LABEL_XOFFSET
	point_label_yoffset = DEF_POINT_LABEL_YOFFSET
	for arg, value in kwargs.items():
		if arg == "point_color":
			point_color = value
		if arg == "point_size":
			point_size = float(value)
		if arg == "point_marker":
			point_marker = value
		if arg == "point_label_visible":
			point_label_visible = value
		if arg == "point_label_color":
			point_label_color = value
		if arg == "point_label_fontsize":
			point_label_fontsize = float(value)
		if arg == "point_label_xoffset":
			point_label_xoffset = float(value)
		if arg == "point_label_yoffset":
			point_label_yoffset = float(value)

	if points:
		num_points = len(points)
		if verbose:
			print(f'''POINT: {num_points} points passed to be plotted\nPOINT: Formating Options:\nPOINT: Point color: {point_color}'''
				f'''\nPOINT: Point size: {point_size}\nPOINT: Point marker: {point_marker}\nPOINT: Label visibility: {point_label_visible}'''
				f'''\nPOINT: Label color: {point_label_color}\nPOINT: Label font size: {point_label_fontsize}'''
				f'''\nPOINT: Label x-offset: {point_label_xoffset}\nPOINT: Label y-offset: {point_label_yoffset}''')

		for point in points:
			x_axis = point[0]
			y_axis = point[1]
			label = point[2]
			
			ax.plot([y_axis],[x_axis], 
				  color=point_color, marker=point_marker)
		   
			if point_label_visible:
				ax.annotate(label, (y_axis + point_label_yoffset, x_axis + point_label_xoffset),
					  horizontalalignment='center', color=point_label_color, fontsize=point_label_fontsize,
					  transform=crs.PlateCarree(), annotation_clip=True, zorder=30)
	
	if kwargs.get('plot_title'):
		plt_title = kwargs.get('plot_title')
		if verbose:
			print("FILE: Plot title set manually to '" + plt_title + "'")
	else:
		plt_title = orbital_slot.replace("-Test", "") + " (" + sat_id.replace("G", "GOES-") + ") - " + human_product_name #ex: GOES-WEST (G16) - Day Cloud Phase
		if verbose:
			print("FILE: Plot title generated dynamically")
	
	plt.title(plt_title, loc='left', fontweight='bold', fontsize=15)
	plt.title('{}'.format(scan_end.strftime('%d %B %Y %H:%M:%S UTC ')), loc='right')
	
	file_name = sat_id + "_" + product + "_" + scan_end.strftime('%Y%m%d_%H%M%S%Z')
	
	if not os.path.exists(save_dir):
		os.makedirs(save_dir)

	dest_path = os.path.join(save_dir, file_name + ".png")
	
	file_dpi = DEF_FILE_DPI
	for arg, value in kwargs.items():
		if arg == "file_dpi":
			file_dpi = int(arg)

	plt.savefig(dest_path, bbox_inches="tight", dpi=file_dpi)
	if verbose:
		print("FILE: File titled " + file_name + " saved to " + save_dir)
		print("FILE: DPI: " + str(file_dpi))
		print("FILE: Full path: " + dest_path)
	plt.close()

	elapsed_time = datetime.now() - start_time
	return 1, elapsed_time.total_seconds(), dest_path

def _calculate_composite_product_data(data, product_name):

	if product_name == 'day_land_cloud_fire':

		red = data['CMI_C06'].data
		green = data['CMI_C03'].data
		blue = data['CMI_C02'].data

		red_bounds = (0.0, 1.0) #in albedo (%)
		green_bounds = (0.0, 1.0) #in albedo (%)
		blue_bounds = (0.0, 1.0) #in albedo (%)

		red = ((red - red_bounds[0]) / (red_bounds[1] - red_bounds[0]))
		green = ((green - green_bounds[0]) / (green_bounds[1] - green_bounds[0]))
		blue = ((blue - blue_bounds[0]) / (blue_bounds[1] - blue_bounds[0]))

		pallete = None

		human_product_name = "Day Land Cloud/Fire"

	if product_name == 'day_cloud_phase':

		red = data['CMI_C13'].data
		green = data['CMI_C02'].data
		blue = data['CMI_C05'].data

		red = red - 273.15 #convert from kelvin to celsius

		red_bounds = (-53.5, 7.5) #in degrees C
		green_bounds = (0.0, 0.78) #in albedo (%)
		blue_bounds = (0.01, 0.59) #in albedo (%)

		#normalize
		red = ((red - red_bounds[1]) / (red_bounds[0] - red_bounds[1]))
		green = ((green - green_bounds[0]) / (green_bounds[1] - green_bounds[0]))
		blue = ((blue - blue_bounds[0]) / (blue_bounds[1] - blue_bounds[0]))

		pallete = None

		human_product_name = 'Day Cloud Phase'

	if product_name == 'nt_microphysics':

		c15 = data['CMI_C15'].data #12.4 micron
		c13 = data['CMI_C13'].data #10.4 micron
		c7 = data['CMI_C07'].data #3.9 micron

		red = c15 - c13
		green = c13 - c7
		blue = c13

		#red = red - 273.15
		#green = green - 273.15
		blue = blue - 273.15

		red_bounds = (-6.7, 2.6)
		green_bounds = (-3.1, 5.2)
		blue_bounds = (-29.6, 19.5)

		red = ((red - red_bounds[0]) / (red_bounds[1] - red_bounds[0]))
		green = ((green - green_bounds[0]) / (green_bounds[1] - green_bounds[0]))
		blue = ((blue - blue_bounds[0]) / (blue_bounds[1] - blue_bounds[0]))

		pallete = None

		human_product_name = "Nighttime Microphysics"
 
	return red, green, blue, pallete, human_product_name

if __name__ == "__main__":

	input_files = ["/Users/rpurciel/Development/lib tests/goes/OR_ABI-L2-MCMIPC-M6_G16_s20230031826175_e20230031828553_c20230031829067.nc"]

	input_save_dir = "/Users/rpurciel/Development/lib tests/goes/raw"

	input_product = "day_land_cloud_fire"

	# input_bbox = [-166.1647, -150.939, 65.7035, 75.5585]

	# #input_bbox = [-180, 180, -85, 85]

	# input_points = [(70.653972, -158.55984, 'Accident'), (70.467394, -157.429593, 'PATQ'), 
	# 				(71.284861, -156.768583, 'PABR'), (70.638000, -159.994750, 'PAWI')]

	# #input_files = sorted(glob.glob(input_file_dir + "*.nc"))
	# num_input_files = len(input_files)

	# tot_files = 0
	# tot_time = 0
	# plot_times = []

	# for input_file_path in input_files:

	# 	status, time, path = plot_composite_goes(input_file_path, input_save_dir, input_product, [], DEF_BBOX, debug=True)

	# 	tot_files += status
	# 	tot_time += time
	# 	plot_times += [time]

	# 	print(f'{tot_files/num_input_files}')

	# tot_time_sec = round(tot_time % 60)
	# tot_time_min = round(tot_time/60)
	# tot_time_hr = round(tot_time/3600)

	# print(f"Finished plotting {tot_files} plots in {tot_time_hr} hours {tot_time_min} minutes {tot_time_sec} seconds.")

	input_hours = range(0, 24, 1)

	input_jd = 201

	input_year = 2023

	input_satellite = "goes16"

	tot_files = 0
	tot_time = 0
	plot_times = []

	for hour in input_hours:

		status, time, path = download_goes(input_save_dir, input_year, input_jd, hour, input_satellite, debug=True)

		tot_files += status
		tot_time += time
		plot_times += [time]

		print(f'{tot_files} downloaded. Est. time remaining')

	print("Done!")






