#a = [2,7,4,0,9,5,1,3]
#n=8
a = [-1,0,1,2,-1,-4]
n=0
a.sort()
inter = []
ans1 = []
interstring=[]
i=0
j=1
for i in range(0,len(a)):
    while(j<len(a)):
        if((a[i]+a[j])<n):
            k = n-(a[i]+a[j])
            if(k in a and k!=a[i] and k!=a[j] and a[i]!=a[j]):
                inter.extend([a[i],a[j],k])
                inter.sort()
                interstr = [str(i) for i in inter]
                ds = "".join(interstr)
                if(ds not in interstring):
                    interstring.append(ds)
                    ans1.append(inter)
                inter=[]
        j = j + 1
    j=i+1
print(ans1)