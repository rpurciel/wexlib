
def time_remaining_calc(tot_items, processed_items, proc_times_list):

	if processed_items <= 1:
		avg_time = 0
	else:
		avg_time = sum(proc_times_list) / len(proc_times_list)

	time_remaining = (avg_time * (tot_items - processed_items))/3600 #in hours
	tr_hours = int(time_remaining)
	tr_minutes = (time_remaining*60) % 60
	tr_seconds = (time_remaining*3600) % 60

	time_remaining_str = "{}:{}:{}".format(str(round(tr_hours)).zfill(2), str(round(tr_minutes)).zfill(2), str(round(tr_seconds)).zfill(2))

	return time_remaining_str