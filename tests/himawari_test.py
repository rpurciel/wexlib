import sys
import glob
from datetime import datetime
import os
import warnings

warnings.filterwarnings('ignore')
sys.path.insert(0, '/Users/ryanpurciel/Development/wexlib') #FOR TESTING ONLY!!!

from util.processing_launchpad import processor_selector

PLATFORM = "Himawari"

print(f"Starting tests for {PLATFORM}")

wd = os.getcwd()

platform_dir = f"{PLATFORM}_cmd"

test_cmd_dir = os.path.join(wd, platform_dir)

test_cmds = sorted(glob.glob(test_cmd_dir + "/*.xml"))

total_start = datetime.now()

for cmd in test_cmds:

	cmd_start = datetime.now()

	print(f"Running command {cmd}")

	processor_selector(cmd, debug=True)

	cmd_time = datetime.now() - cmd_start

	print(f"Finished command {cmd} in {cmd_time.total_seconds()} sec")

total_time = datetime.now() - total_start

print(f"Finished {PLATFORM} tests in {total_time.total_seconds()} sec")

