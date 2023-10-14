from model import hrrr

OUTPUT_DIRECTORY = "/Users/rpurciel/WeatherExtreme Ltd Dropbox/Ryan Purciel/Scripts/~GH Repo/scripts/lib/out"

if __name__ == "__main__":

	paths = []
	tot_time = 0

	#first test: download HRRR model data
	print("base")
	dl_result, dl_time, dl_path = hrrr.download_hrrr(OUTPUT_DIRECTORY, 2023, 9, 2, 18)
	paths += [dl_path]
	tot_time += dl_time
	print(dl_result, dl_time, dl_path)

	#download AK data
	print("ak")
	dl_result, dl_time, dl_path = hrrr.download_hrrr(OUTPUT_DIRECTORY, 2023, 9, 2, 18, alaska=True)
	paths += [dl_path]
	tot_time += dl_time
	print(dl_result, dl_time, dl_path)

	#Download data with specified forecast hour
	print("base+fcst hr")
	dl_result, dl_time, dl_path = hrrr.download_hrrr(OUTPUT_DIRECTORY, 2023, 9, 2, 18, forecast_hour=6)
	paths += [dl_path]
	tot_time += dl_time
	print(dl_result, dl_time, dl_path)

	#download alaska data with specified fcst hr
	print("ak+fcst hr")
	dl_result, dl_time, dl_path = hrrr.download_hrrr(OUTPUT_DIRECTORY, 2023, 9, 2, 18, forecast_hour=6, alaska=True)
	paths += [dl_path]
	tot_time += dl_time
	print(dl_result, dl_time, dl_path)

	print("\ndone. total downloading time: ", round(tot_time/60), " minutes ", round(tot_time % 60,2), " seconds")

	#second test: turn all dl'd files into RAOB csv's
	for path in paths:

		print("using path ", path)

		csv_result, csv_time, csv_path = hrrr.raobcsv_sounding_from_hrrr(path, OUTPUT_DIRECTORY)
		print(csv_result, csv_time, csv_path)


