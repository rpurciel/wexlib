import sys
import os
import glob
from datetime import datetime

def download_model(save_dir, year, month, day, hour, **kwargs):
	"""
    Downloads a single model file to a local directory. 

    Inputs: Directory to save model output files to, and year, month,
            day, and hour to download model file for.

    Returns: Success code, time (in seconds) for function to run,
             path to file
    """
	start_time = datetime.now()

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




