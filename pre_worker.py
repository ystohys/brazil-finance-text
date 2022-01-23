import os
import sys
import subprocess

# Splitting up of the whole directory is done here
full_file_list = os.listdir('bids/txt/')

# simple_list = [['153166_612015.txt', '153166_632006.txt'],['153166_632008.txt', '153166_632012.txt']]

batch_size = 5000
batches_list = [full_file_list[i:i+batch_size] for i in range(0, len(full_file_list), batch_size)]

for batch_num, each_batch in enumerate(batches_list):
    result = subprocess.run([sys.executable, 'main_worker.py', str(each_batch), 'batch{0}'.format(batch_num)],
                            capture_output=True, text=True)

    print(result.stdout)
    print(result.stderr)