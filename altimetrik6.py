from pyspark import SparkContext
sc = SparkContext()

s = sc.parallelize(["oikapakmz","banana","oppo"])

def fun(s):
    l = len(s)
    i=0
    j=len(s)
    maxlen = 0
    maxpal = ""
    while(i<len(s)):
        while(j>i):
            stnew = s[i:j]
            if(stnew==stnew[::-1]):
                if(len(stnew)>maxlen):
                    maxlen = len(stnew)
                    maxpal = stnew
            print(stnew)
            j=j-1
        i=i+1
        j=len(s)
    print(">>>>>>>>>>>>>>>>",maxpal,maxlen)
    return maxlen
lst = s.map(lambda x:fun(x))
print(lst.collect())
