import threading
import subprocess

class MyClass(threading.Thread):
    def __init__(self):
        self.stdout = None
        self.stderr = None
        threading.Thread.__init__(self)

    def run(self):
        p = subprocess.Popen("python C:/Users/RAHUL/PycharmProjects/SparkProject/Jio/r1.py",
                             shell=True,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)

        self.stdout, self.stderr = p.communicate()

myclass = MyClass()
myclass.start()
myclass.join()
print(myclass.stdout)