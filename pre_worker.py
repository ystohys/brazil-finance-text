import os
import sys
import subprocess


def pre_main_run(folder_path, batch_size):
    full_file_list = os.listdir(folder_path)
    batches_list = [full_file_list[i:i+batch_size] for i in range(0, len(full_file_list), batch_size)]
    for batch_num, each_batch in enumerate(batches_list):
        result = subprocess.run([sys.executable,
                                 'main_worker.py',
                                 folder_path,
                                 str(each_batch),
                                 'batch{0}'.format(batch_num+1)],
                                capture_output=True, text=True)
        print(result.stdout)
        print(result.stderr)


if __name__ == '__main__':
    pre_main_run(sys.argv[1], int(sys.argv[2]))

# simple_list = [['153166_612015.txt', '153166_632006.txt'],['153166_632008.txt', '153166_632012.txt'],
#                ['10001_372015.txt', '10001_392015.txt']]
