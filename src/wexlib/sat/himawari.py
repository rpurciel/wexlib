import sys
import os
import glob
from datetime import datetime, timedelta
import re
import glob
import warnings

import xarray as xr
import numpy as np
import metpy
import matplotlib
from matplotlib import cm
import matplotlib.pyplot as plt
import matplotlib.patheffects as PathEffects
import cartopy 
import cartopy.crs as crs
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import cartopy.io.shapereader as shpreader
from cartopy.feature import NaturalEarthFeature
import s3fs
from satpy import Scene
from pyresample.utils import get_area_def

import wexlib.util.internal as internal

warnings.filterwarnings('ignore')

#global params
DEF_BBOX = [50.8, -65.2, 24.1, -126.3] #CONUS - NESW

DEF_VMIN = None
DEF_VMAX = None

DEF_GEOG_VISIBLE = True
DEF_GEOG_DRAW_STATES = True
DEF_GEOG_DRAW_COASTLINES = True
DEF_GEOG_DRAW_BORDERS = True
DEF_GEOG_DRAW_WATER = False

DEF_COLORBAR_VISIBLE = False
DEF_COLORBAR_LABEL = 'Pixel Brightness'
DEF_COLORBAR_SHRINK_FACT = 0.5
DEF_COLORBAR_LOCATION = 'inside'

DEF_POINT_COLOR = 'black'
DEF_POINT_SIZE = 8
DEF_POINT_MARKER = 'o'
DEF_POINT_LABEL_VISIBLE = True
DEF_POINT_LABEL_COLOR = 'black'
DEF_POINT_LABEL_FONTSIZE = 10
DEF_POINT_LABEL_FONTWEIGHT = 400
DEF_POINT_LABEL_OUTLINE = True 
DEF_POINT_LABEL_OUTLINE_COLOR = 'white'
DEF_POINT_LABEL_OUTLINE_WEIGHT = 5
DEF_POINT_LABEL_XOFFSET = -0.85
DEF_POINT_LABEL_YOFFSET = 0

DEF_FILE_DPI = 300

DEF_TRUE_COLOR_PALLETE = None
DEF_NATURAL_COLOR_PALLETE = None
DEF_DAY_CLOUD_PHASE_PALLETE = None
DEF_DAY_LAND_CLOUD_FIRE_PALLETE = None
DEF_NT_MICROPHYSICS_PALLETE = None

#download params
DEF_PRODUCT = 'FLDK' #Full Disk
DEF_SATELLITE = "himawari8"

#single-band params
DEF_SB_PALLETE = 'Greys_r'

BAND_NAMES = {
	'B01' : 'Blue [0.46 μm]',
	'B02' : 'Green [0.51 μm]',
	'B03' : 'Red [0.64 μm]',
	'B04' : 'Veggie [0.86 μm]',
	'B05' : 'Snow [1.6 μm]',
	'B06' : 'Cloud Top Phase [2.3 μm]',
	'B07' : 'Shortwave IR [3.9 μm]',
	'B08' : 'Upper-level Water Vapor [6.2 μm]',
	'B09' : 'Mid-level Water Vapor [6.9 μm]',
	'B10' : 'Lower-level Water Vapor [7.3 μm]',
	'B11' : 'Midwave IR [8.6 μm]',
	'B12' : 'Ozone [9.6 μm]',
	'B13' : '"Clean" Longwave IR [10.4 μm]',
	'B14' : 'Longwave IR [11.2 μm]',
	'B15' : '"Dirty" Longwave IR [12.4 μm]',
	'B16' : 'CO2 [13.3 μm]',
}

def aws_read_bucket(year, month, day, hour, minute, satellite=DEF_SATELLITE, product=DEF_PRODUCT, **kwargs):
	"""
	Downloads a single model file to a local directory. 

	Inputs: Directory to save model output files to, and year, month,
			day, and hour to download model file for.

	Returns: Success code, time (in seconds) for function to run,
			 path to file
	"""

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

	try:
		aws = s3fs.S3FileSystem(anon=True)
	except Exception as e:
		error_str = ("Error: " + e)

		if verbose:
			print(error_str)

		return 0, error_str

	month = str(month).zfill(2)
	day = str(day).zfill(2)
	hour = str(hour).zfill(2)
	minute = str(minute).zfill(2)

	try:
		list_of_aws_urls = np.array(aws.ls(f'noaa-{satellite}/AHI-L1b-{product}/{year}/{month}/{day}/{hour}{minute}'))
	except Exception as e:
		error_str = ("ERROR: ", e)

		if verbose:
			print("ERROR:", e)

		return 0, error_str

	if verbose:
		print(f"INFO: Total = {len(list_of_aws_urls)} files")
		print(f'''INFO: Parameters:\nINFO: Year = {year}\nINFO: Month = {month}\nINFO: Day = {day}\n'''
			  f'''INFO: Hour = {hour}\nINFO: Minute = {minute}\nINFO: Satellite = {satellite}\nINFO: Sector = {product}''')
	if debug:
		print("DEBUG: Files to download:")
		print(list_of_aws_urls)

	return 1, list_of_aws_urls

def download_single_file_aws(save_dir, aws_url, **kwargs):
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

def plot_single_band(path_to_sectors_and_band_files, save_dir, band, points, bbox=DEF_BBOX, pallete=DEF_SB_PALLETE, **kwargs):
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

	bbox_WSEN = (float(bbox[3]), float(bbox[2]), float(bbox[1]), float(bbox[0])) #NESW to WSEN

	bbox_WESN = (float(bbox[3]), float(bbox[1]), float(bbox[2]), float(bbox[0])) #NESW to WSEN

	if not os.path.exists(save_dir):
		os.makedirs(save_dir)

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

	if verbose:
		print("INFO: Loading scene...")

	base_scene = Scene(filenames=path_to_sectors_and_band_files, reader='ahi_hsd')

	if band < 1 or band > 16:
		error_str = "Error: Selected band must be between 1-16 (Selected band: " + str(band) + "?)"
		if verbose:
			print("INFO: " + error_str)

		elapsed_time = datetime.now() - start_time
		return 0, elapsed_time.total_seconds(), error_str

	sel_band_str = 'B' + str(band).zfill(2)
	base_scene.load([sel_band_str])

	if verbose:
		print("INFO: Scene loaded")
		print("INFO: Selected band: ", sel_band_str)
		print("INFO: Cropping and resampling...")

	base_scene = base_scene.crop(ll_bbox=bbox_WSEN) # WSEN

	resampled_scene = base_scene.resample(base_scene.min_area(), resampler='native')

	if verbose:
		print("INFO: Cropped and resampled")

	scan_start = resampled_scene[sel_band_str].attrs['start_time']
	scan_end = resampled_scene[sel_band_str].attrs['end_time']
	sat_name = resampled_scene[sel_band_str].attrs['platform_name']
	sat_id = sat_name.replace("Himawari-", "hw")

	band_data = resampled_scene[sel_band_str].values

	if verbose:
		print(f"""INFO: Scan start: {scan_start}\nINFO: Scan end: {scan_end}"""
			  f"""\nINFO: Satellite: {sat_name}""")

	if debug:
		print("DEBUG: Band data:")
		print(band_data)

	ccrs = resampled_scene[sel_band_str].attrs['area'].to_cartopy_crs()

	fig = plt.figure(figsize=(16., 9.))
	ax = fig.add_subplot(1, 1, 1, projection=ccrs)

	image_vmin = DEF_VMIN
	image_vmax = DEF_VMAX

	for arg, value in kwargs.items():
		if arg == 'vmin':
			image_vmin = float(value)
		if arg == 'vmax':
			image_vmax = float(value)

	if image_vmax and image_vmin:
		img = ax.imshow(band_data, extent=ccrs.bounds, transform=ccrs, interpolation='none', cmap=pallete, vmin=image_vmin, vmax=image_vmax)
	else:
		img = ax.imshow(band_data, extent=ccrs.bounds, transform=ccrs, interpolation='none', cmap=pallete)
	
	#COLORBAR

	colorbar_visible = DEF_COLORBAR_VISIBLE
	colorbar_label = DEF_COLORBAR_LABEL
	colorbar_shrink_fact = DEF_COLORBAR_SHRINK_FACT
	colorbar_location = DEF_COLORBAR_LOCATION
	for arg, value in kwargs.items():
		if arg == 'colorbar_visible':
			colorbar_visible = value
		if arg == 'colorbar_label':
			colorbar_label = value
		if arg == 'colorbar_shrink_fact':
			colorbar_shrink_fact = float(value)
		if arg == 'colorbar_location':
			colorbar_location = value

	if colorbar_visible:
		if colorbar_location == 'inside':
			plt.colorbar(mappable=img, ax=ax, orientation = "horizontal", shrink=colorbar_shrink_fact, pad=-0.15).set_label(colorbar_label)
			if verbose:
				print(f"COLORBAR: Drawing colorbar turned ON\nCOLORBAR: Location = Inside plot\nCOLORBAR: Label = '{colorbar_label}'")
		if colorbar_location == 'bottom':
			plt.colorbar(mappable=img, ax=ax, orientation = "horizontal", shrink=colorbar_shrink_fact, pad=.05).set_label(colorbar_label)
			if verbose:
				print(f"COLORBAR: Drawing colorbar turned ON\nCOLORBAR: Location = Bottom of plot\nCOLORBAR: Label = '{colorbar_label}'")
		if colorbar_location == 'right':
			plt.colorbar(mappable=img, ax=ax, orientation = "vertical", shrink=colorbar_shrink_fact, pad=.05).set_label(colorbar_label)
			if verbose:
				print(f"COLORBAR: Drawing colorbar turned ON\nCOLORBAR: Location = Left of plot\nCOLORBAR: Label = '{colorbar_label}'")
	
	#GEOGRAPHY DRAWING

	geog_visible = DEF_GEOG_VISIBLE
	geog_draw_states = DEF_GEOG_DRAW_STATES
	geog_draw_coastlines = DEF_GEOG_DRAW_COASTLINES
	geog_draw_borders = DEF_GEOG_DRAW_BORDERS
	geog_draw_water = DEF_GEOG_DRAW_WATER
	for arg, value in kwargs.items():
		if arg == "geog_visible":
			geog_visible = value
		if arg == "geog_draw_states":
			geog_draw_states = value
		if arg == "geog_draw_coastlines":
			geog_draw_coastlines = value
		if arg == "geog_draw_borders":
			geog_draw_borders = value
		if arg == "geog_draw_water":
			geog_draw_water = value
	#TODO: Add in more geography drawing options

	if geog_visible:
		if geog_draw_states:
			ax.add_feature(crs.cartopy.feature.STATES)
			if verbose: 
				print("GEOG: Drawing states")
		if geog_draw_coastlines:
			ax.coastlines(resolution='50m', color='black', linewidth=1)
			if verbose:
				print("GEOG: Drawing coastlines")
		if geog_draw_borders:
			ax.add_feature(cfeature.BORDERS, linewidth=1.5)
			if verbose:
				print("GEOG: Drawing borders")
		if geog_draw_water:
			ax.add_feature(cfeature.LAKES.with_scale('10m'),linestyle='-',linewidth=0.5,alpha=1,edgecolor='blue',facecolor='none')
			if verbose:
				print("GEOG: Drawing water")
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
	point_label_fontweight = DEF_POINT_LABEL_FONTWEIGHT
	point_label_xoffset = DEF_POINT_LABEL_XOFFSET
	point_label_yoffset = DEF_POINT_LABEL_YOFFSET
	point_label_outline = DEF_POINT_LABEL_OUTLINE
	point_label_outline_color = DEF_POINT_LABEL_OUTLINE_COLOR
	point_label_outline_weight = DEF_POINT_LABEL_OUTLINE_WEIGHT
	for arg, value in kwargs.items():
		if arg == "point_color":
			point_color = value
		if arg == "point_size":
			point_size = float(value)
		if arg == "point_marker":
			point_marker = value
		if arg == "point_label_visible":
			point_label_visible = internal.str_to_bool(value)
		if arg == "point_label_color":
			point_label_color = value
		if arg == "point_label_fontweight":
			point_label_fontweight = float(value)
		if arg == "point_label_fontsize":
			point_label_fontsize = float(value)
		if arg == "point_label_xoffset":
			point_label_xoffset = float(value)
		if arg == "point_label_yoffset":
			point_label_yoffset = float(value)
		if arg == "point_label_outline":
			point_label_outline = internal.str_to_bool(value)
		if arg == "point_label_outline_color":
			point_label_outline_color = value
		if arg == "point_label_outline_weight":
			point_label_outline_weight = float(value)

	if points:
		num_points = len(points)
		if verbose:
			print(f'''POINT: {num_points} points passed to be plotted\nPOINT: Formating Options:\nPOINT: Point color: {point_color}'''
				f'''\nPOINT: Point size: {point_size}\nPOINT: Point marker: {point_marker}\nPOINT: Label visibility: {point_label_visible}'''
				f'''\nPOINT: Label color: {point_label_color}\nPOINT: Label font size: {point_label_fontsize}\nPOINT: Label font weight: {point_label_fontweight}'''
				f'''\nPOINT: Label x-offset: {point_label_xoffset}\nPOINT: Label y-offset: {point_label_yoffset}\nPOINT: Label outline: {point_label_outline}'''
				f'''\nPOINT: Label outline color: {point_label_outline_color}\nPOINT: Label outline weight: {point_label_outline_weight}''')

		for point in points:
			x_axis = point[0]
			y_axis = point[1]
			label = point[2]
			
			ax.plot(y_axis, x_axis, 
				  color=point_color, marker=point_marker, markersize=point_size,
				  transform=crs.PlateCarree())
		   
			if point_label_visible:
				label_obj = ax.annotate(label, (y_axis + point_label_yoffset, x_axis + point_label_xoffset),
					  horizontalalignment='center', color=point_label_color, fontsize=point_label_fontsize,
					  fontweight=point_label_fontweight, transform=crs.PlateCarree(), annotation_clip=True)

				if point_label_outline:
					label_obj.set_path_effects([PathEffects.withStroke(linewidth=point_label_outline_weight, foreground=point_label_outline_color)])


	if internal.str_to_bool(kwargs.get('plot_simple_band')) == True:
		band_name = BAND_NAMES.get(sel_band_str)[:-10]
	else:
		band_name = BAND_NAMES.get(sel_band_str)

	if kwargs.get('plot_title'):
		plt_title = kwargs.get('plot_title')
		if verbose:
			print("FILE: Plot title set manually to '" + plt_title + "'")
	else:
		plt_title = sat_name + " - " + band_name
		if verbose:
			print("FILE: Plot title generated dynamically")
	
	plt.title(plt_title, loc='left', fontweight='bold', fontsize=15)
	plt.title('{}'.format(scan_end.strftime('%d %B %Y %H:%M:%S UTC ')), loc='right')
	
	file_name = sat_id + "_" + sel_band_str.replace("B", "b") + "_" + scan_end.strftime('%Y%m%d_%H%M%S%Z')
	
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

def plot_composite(path_to_sectors_and_band_files, save_dir, product, points, bbox=DEF_BBOX, **kwargs):
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

	bbox_WSEN = (float(bbox[3]), float(bbox[2]), float(bbox[1]), float(bbox[0])) #NESW to WSEN

	bbox_WESN = (float(bbox[3]), float(bbox[1]), float(bbox[2]), float(bbox[0])) #NESW to WSEN

	if not os.path.exists(save_dir):
		os.makedirs(save_dir)

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

	if verbose:
		print("INFO: Loading scene...")

	base_scene = Scene(filenames=path_to_sectors_and_band_files, reader='ahi_hsd')

	base_scene.load([product])

	if verbose:
		print("INFO: Scene loaded")

	pallete, human_product_name = _calculate_composite_product_data(product)

	if verbose:
		print(f"INFO: Selected product: {product}")
		print(f"INFO: Natural name: {human_product_name}")
		print(f"INFO: Auto-selected pallete: {pallete}")
		print("INFO: Cropping and resampling...")

	base_scene = base_scene.crop(ll_bbox=bbox_WSEN) # WSEN

	resampled_scene = base_scene.resample(base_scene.min_area(), resampler='native')

	if verbose:
		print("INFO: Cropped and resampled")

	scan_start = resampled_scene[product].attrs['start_time']
	scan_end = resampled_scene[product].attrs['end_time']
	sat_name = resampled_scene[product].attrs['platform_name']
	sat_id = sat_name.replace("Himawari-", "hw")

	image = np.asarray(resampled_scene[product]).transpose(1,2,0)
	image = np.interp(image, (np.percentile(image,1), np.percentile(image,99)), (0, 1))

	if verbose:
		print(f"""INFO: Scan start: {scan_start}\nINFO: Scan end: {scan_end}"""
			  f"""\nINFO: Satellite: {sat_name}""")

	if debug:
		print("DEBUG: Image data:")
		print(image)

	ccrs = resampled_scene[product].attrs['area'].to_cartopy_crs()

	fig = plt.figure(figsize=(16., 9.))
	ax = fig.add_subplot(1, 1, 1, projection=ccrs)
	
	img = ax.imshow(image, extent=ccrs.bounds, transform=ccrs, interpolation='none')
	
	#COLORBAR

	colorbar_visible = DEF_COLORBAR_VISIBLE
	colorbar_label = DEF_COLORBAR_LABEL
	colorbar_shrink_fact = DEF_COLORBAR_SHRINK_FACT
	colorbar_location = DEF_COLORBAR_LOCATION
	for arg, value in kwargs.items():
		if arg == 'colorbar_visible':
			colorbar_visible = value
		if arg == 'colorbar_label':
			colorbar_label = value
		if arg == 'colorbar_shrink_fact':
			colorbar_shrink_fact = float(value)
		if arg == 'colorbar_location':
			colorbar_location = value

	if colorbar_visible:
		if colorbar_location == 'inside':
			plt.colorbar(mappable=img, ax=ax, orientation = "horizontal", shrink=colorbar_shrink_fact, pad=-0.15).set_label(colorbar_label)
			if verbose:
				print(f"COLORBAR: Drawing colorbar turned ON\nCOLORBAR: Location = Inside plot\nCOLORBAR: Label = '{colorbar_label}'")
		if colorbar_location == 'bottom':
			plt.colorbar(mappable=img, ax=ax, orientation = "horizontal", shrink=colorbar_shrink_fact, pad=.05).set_label(colorbar_label)
			if verbose:
				print(f"COLORBAR: Drawing colorbar turned ON\nCOLORBAR: Location = Bottom of plot\nCOLORBAR: Label = '{colorbar_label}'")
		if colorbar_location == 'right':
			plt.colorbar(mappable=img, ax=ax, orientation = "vertical", shrink=colorbar_shrink_fact, pad=.05).set_label(colorbar_label)
			if verbose:
				print(f"COLORBAR: Drawing colorbar turned ON\nCOLORBAR: Location = Left of plot\nCOLORBAR: Label = '{colorbar_label}'")
	
	#GEOGRAPHY DRAWING

	geog_visible = DEF_GEOG_VISIBLE
	geog_draw_states = DEF_GEOG_DRAW_STATES
	geog_draw_coastlines = DEF_GEOG_DRAW_COASTLINES
	geog_draw_borders = DEF_GEOG_DRAW_BORDERS
	geog_draw_water = DEF_GEOG_DRAW_WATER
	for arg, value in kwargs.items():
		if arg == "geog_visible":
			geog_visible = value
		if arg == "geog_draw_states":
			geog_draw_states = value
		if arg == "geog_draw_coastlines":
			geog_draw_coastlines = value
		if arg == "geog_draw_borders":
			geog_draw_borders = value
		if arg == "geog_draw_water":
			geog_draw_water = value
	#TODO: Add in more geography drawing options

	if geog_visible:
		if geog_draw_states:
			ax.add_feature(crs.cartopy.feature.STATES)
			if verbose: 
				print("GEOG: Drawing states")
		if geog_draw_coastlines:
			ax.coastlines(resolution='50m', color='black', linewidth=1)
			if verbose:
				print("GEOG: Drawing coastlines")
		if geog_draw_borders:
			ax.add_feature(cfeature.BORDERS, linewidth=1.5)
			if verbose:
				print("GEOG: Drawing borders")
		if geog_draw_water:
			ax.add_feature(cfeature.LAKES.with_scale('10m'),linestyle='-',linewidth=0.5,alpha=1,edgecolor='blue',facecolor='none')
			if verbose:
				print("GEOG: Drawing water")
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
	point_label_fontweight = DEF_POINT_LABEL_FONTWEIGHT
	point_label_xoffset = DEF_POINT_LABEL_XOFFSET
	point_label_yoffset = DEF_POINT_LABEL_YOFFSET
	point_label_outline = DEF_POINT_LABEL_OUTLINE
	point_label_outline_color = DEF_POINT_LABEL_OUTLINE_COLOR
	point_label_outline_weight = DEF_POINT_LABEL_OUTLINE_WEIGHT
	for arg, value in kwargs.items():
		if arg == "point_color":
			point_color = value
		if arg == "point_size":
			point_size = float(value)
		if arg == "point_marker":
			point_marker = value
		if arg == "point_label_visible":
			point_label_visible = internal.str_to_bool(value)
		if arg == "point_label_color":
			point_label_color = value
		if arg == "point_label_fontweight":
			point_label_fontweight = float(value)
		if arg == "point_label_fontsize":
			point_label_fontsize = float(value)
		if arg == "point_label_xoffset":
			point_label_xoffset = float(value)
		if arg == "point_label_yoffset":
			point_label_yoffset = float(value)
		if arg == "point_label_outline":
			point_label_outline = internal.str_to_bool(value)
		if arg == "point_label_outline_color":
			point_label_outline_color = value
		if arg == "point_label_outline_weight":
			point_label_outline_weight = float(value)

	if points:
		num_points = len(points)
		if verbose:
			print(f'''POINT: {num_points} points passed to be plotted\nPOINT: Formating Options:\nPOINT: Point color: {point_color}'''
				f'''\nPOINT: Point size: {point_size}\nPOINT: Point marker: {point_marker}\nPOINT: Label visibility: {point_label_visible}'''
				f'''\nPOINT: Label color: {point_label_color}\nPOINT: Label font size: {point_label_fontsize}\nPOINT: Label font weight: {point_label_fontweight}'''
				f'''\nPOINT: Label x-offset: {point_label_xoffset}\nPOINT: Label y-offset: {point_label_yoffset}\nPOINT: Label outline: {point_label_outline}'''
				f'''\nPOINT: Label outline color: {point_label_outline_color}\nPOINT: Label outline weight: {point_label_outline_weight}''')

		for point in points:
			x_axis = point[0]
			y_axis = point[1]
			label = point[2]
			
			ax.plot(y_axis, x_axis, 
				  color=point_color, marker=point_marker, markersize=point_size,
				  transform=crs.PlateCarree())
		   
			if point_label_visible:
				label_obj = ax.annotate(label, (y_axis + point_label_yoffset, x_axis + point_label_xoffset),
					  horizontalalignment='center', color=point_label_color, fontsize=point_label_fontsize,
					  fontweight=point_label_fontweight, transform=crs.PlateCarree(), annotation_clip=True)

				if point_label_outline:
					label_obj.set_path_effects([PathEffects.withStroke(linewidth=point_label_outline_weight, foreground=point_label_outline_color)])

	
	if kwargs.get('plot_title'):
		plt_title = kwargs.get('plot_title')
		if verbose:
			print("FILE: Plot title set manually to '" + plt_title + "'")
	else:
		plt_title = sat_name + " - " + human_product_name
		if verbose:
			print("FILE: Plot title generated dynamically")
	
	plt.title(plt_title, loc='left', fontweight='bold', fontsize=15)
	plt.title('{}'.format(scan_end.strftime('%d %B %Y %H:%M:%S UTC ')), loc='right')
	
	file_name = sat_id + "_" + product + "_" + scan_end.strftime('%Y%m%d_%H%M%S%Z')

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

def _calculate_composite_product_data(product_name):

	if product_name == 'day_land_cloud_fire':

		pallete = DEF_DAY_LAND_CLOUD_FIRE_PALLETE

		human_product_name = "Day Land Cloud/Fire"

	if product_name == 'day_cloud_phase':

		pallete = DEF_DAY_CLOUD_PHASE_PALLETE

		human_product_name = 'Day Cloud Phase'

	if product_name == 'nt_microphysics':

		pallete = DEF_NT_MICROPHYSICS_PALLETE

		human_product_name = "Nighttime Microphysics"

	if product_name == 'true_color':

		pallete = DEF_TRUE_COLOR_PALLETE

		human_product_name = "True Color"

	if product_name == 'natural_color':

		pallete = DEF_NATURAL_COLOR_PALLETE

		human_product_name = "Natural Color"
 
	return pallete, human_product_name

if __name__ == "__main__":

	input_files = ["/Users/rpurciel/Development/lib tests/goes/OR_ABI-L2-MCMIPC-M6_G16_s20230031826175_e20230031828553_c20230031829067.nc"]

	input_save_dir = "/Users/rpurciel/Development/lib tests/himawari/case/"

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

	input_hours = range(15, 23, 1)

	input_minutes = range(0, 60, 10)

	input_minutes = [50]

	input_day = 6

	input_month = 10

	input_year = 2023

	input_satellite = "himawari9"

	bucket_times = []

	start_time = datetime.now()

	tot_buckets = len(input_hours) + len(input_minutes)
	num_buckets = 1

	tot_files_all_buckets = 0
	all_time = 0

	for hour in input_hours:

		for minute in input_minutes:

			print(f'{num_buckets}/{tot_buckets} AWS buckets indexed.')

			status, files = aws_readbucket_himawari(input_year, input_month, input_day, hour, minute, input_satellite)

			num_buckets += status

			tot_files = len(files)

			num_files = 0
			tot_time = 0
			plot_times = []

			for file in files:

				status, time, path = download_singlefile_aws_himawari(input_save_dir, file)

				num_files += status
				tot_time += time
				plot_times += [time]

				time_remaining = time_remaining_calc(tot_files, num_files, plot_times)

				print(f'{num_files}/{tot_files} files downloaded from bucket. Est. time remaining: {time_remaining}', end='\r')

			tot_time_str = total_time_calc(tot_time)

			tot_files_all_buckets += num_files

			bucket_times += [tot_time]
			all_time += tot_time

			bucket_time_est = time_remaining_calc(tot_buckets, num_buckets, bucket_times)

			print(f'Finished downloading {num_files} files from bucket. Total time: {tot_time_str}         ')
			print(f'Estimated time remaining: {bucket_time_est}\n')

	all_time_str = total_time_calc(all_time)

	tot_files_all_buckets += num_files

	print(f'Finished downloading {tot_files_all_buckets} files from {num_buckets} buckets. Total time: {all_time_str}')



	# for hour in input_hours:

	# 	for minute in input_minutes:

	# 		status, time, path = download_goes(input_save_dir, input_year, input_month, input_day, hour, minute, input_satellite, debug=True)

	# 		tot_files += status
	# 		tot_time += time
	# 		plot_times += [time]

	# 		print(f'{tot_files} downloaded. Est. time remaining')

	# elapsed_time = datetime.now() - start_time

	# elapsed_sec = round(elapsed_time.total_seconds() % 60, 2)
	# elapsed_min = round((elapsed_time / timedelta(minutes=1)) % 60)
	# elapsed_hr = floor((elapsed_time / timedelta(hours=1)) % 24)

	# print("Done!")
	# print(f"Took {elapsed_hr} hours {elapsed_min} minutes {elapsed_sec} seconds to download {tot_files} files.")






