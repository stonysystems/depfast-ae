import subprocess
import matplotlib.pyplot as plt
import sys
import numpy as np

big_arr = []
for i in range(1,10):
    stdout = subprocess.check_output("cat " + sys.argv[1] + "_" + str(i) + "-tpca_2pl_ww-fpga_raft_1_1_-1.log  | grep Total | awk 'NR % 3 == 2' | awk '{print $7}'", shell=True)
    lines = stdout.splitlines()
    print(len(lines)-2)
    temp = []
    x_axis = []
    j = 0
    for i in range(29):
        x_axis.append(j)
        j += 5
        temp.append(int(lines[i]))
    x_axis = x_axis[1:]
    x_axis = x_axis[:-1]
    temp = temp[1:]
    temp = temp[:-1]
    '''big_arr.append(temp)

    arr = np.array(big_arr)
    arr = np.average(arr, axis=0)
    print(arr)'''

    plt.plot(x_axis, arr)
    plt.savefig(sys.argv[1] + "_" + str(i) + ".png")
