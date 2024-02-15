from datetime import datetime

from processorInstantiator import _ProcessorInstantiator
from pointPath import PointPath

def create(storage_dir_path, product):
    return _ProcessorInstantiator(storage_dir_path, product)

if __name__ == "__main__":

    #ipython compatability
    __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"

    # new_proc = create('/Users/rpurciel/Development/NON GH/test', "gfs")

    start_time = datetime(2022, 4, 13, 6, 0)
    end_time = datetime(2022, 4, 13, 18, 0)

    # new_proc.archive_download(start_time, end_time)

    hrrr_proc = create('/Users/rpurciel/Documents/Cessna 208B Idaho/Vector KMZs/', "hrrr")

    hrrr_proc.archive_download(start_time, end_time)

    #hrrr_proc.load_chunk('a7214c13-1beb-4968-894a-a51727545af8')

    # fp = PointPath(from_csv="/Users/rpurciel/Documents/Solis v RAPCO/flight_path.csv")

    # #print(fp.times)

    # hrrr_proc.sounding('cross-section', fp, force_time="2020-03-13 21:00:00")


















