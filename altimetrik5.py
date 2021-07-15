s = ["oikapakmz","banana"]
s2 = "banana"

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