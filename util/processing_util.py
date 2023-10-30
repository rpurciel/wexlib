import math

def calc_time_remaining(tot_items, processed_items, proc_times_list):

	if processed_items <= 1:
		avg_time = 0
	else:
		avg_time = sum(proc_times_list) / len(proc_times_list)

	time_remaining = (avg_time * (tot_items - processed_items)) #in hours
	tr_hours = math.floor((time_remaining/3600) % 60)
	tr_minutes = math.floor((time_remaining/60) % 60)
	tr_seconds = round(time_remaining % 60)

	time_remaining_str = "{}:{}:{}".format(str(round(tr_hours)).zfill(2), str(round(tr_minutes)).zfill(2), str(round(tr_seconds)).zfill(2))

	return time_remaining_str

def calc_total_time(total_time_seconds):

	t_hours = math.floor((total_time_seconds/3600) % 60)
	t_minutes = math.floor((total_time_seconds/60) % 60)
	t_seconds = round(total_time_seconds % 60)

	time_str = "{}:{}:{}".format(str(round(t_hours)).zfill(2), str(round(t_minutes)).zfill(2), str(round(t_seconds)).zfill(2))

	return time_str
