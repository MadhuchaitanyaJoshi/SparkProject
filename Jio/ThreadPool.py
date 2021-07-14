from multiprocessing.pool import ThreadPool
import subprocess
import psutil
import time
all_samples = ["python C:/Users/RAHUL/PycharmProjects/SparkProject/Jio/r1.py","python C:/Users/RAHUL/PycharmProjects/SparkProject/Jio/r2.py","python C:/Users/RAHUL/PycharmProjects/SparkProject/Jio/r3.py"]
rs = []
start=time.perf_counter()

def work(sample):
    my_tool_subprocess = subprocess.Popen(sample,shell=True, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    print(my_tool_subprocess.pid)
    rs.append(my_tool_subprocess.pid)
    line = True
    while line:
        print("here in search of stdout","------->",line)
        myline = my_tool_subprocess.stdout.readline()
        line=False
        #here I parse stdout..
    print("completed function")

num = 3  # set to the number of workers you want (it defaults to the cpu count of your machine)
tp = ThreadPool(num)
for sample in all_samples:
    tp.apply_async(work, (sample,))
print("after for loop")

tp.close()
print("after close")
tp.join()
print("end")
print(rs)

while True:
    count = 0
    for pid in rs:
        if(pid in psutil.pids()):
            print(pid," is active")

        else:
            count = count + 1
            print(pid," is not active")
    if count==len(rs):
        break
end = time.perf_counter()
print("final req time is",end-start)