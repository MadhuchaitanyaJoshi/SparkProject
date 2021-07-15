s = "oikapakmz"
n = 0
i=0
flag=1
j=len(s)
print(s[i:j],s[::-1])
len1=0
len2 = 0
pivotstart = i
pivotend = j
cnt=1
while(flag==1 and pivotstart!=pivotend):
    if(s[i:j]==s[::-1]):
        print(s[i:j])
        break
    else:
        i=pivotstart
        j=pivotend
        i=i+1
        while(i<j):
            print("inthe first")
            print(i,s[i:j],s[j:i:-1],j)
            if(s[i:j]==s[j:i-1:-1]):
                print("matched",s[i:j])
                len1=len(s[i:j])
                len1 = max(len1,len(s[i:j]))
                print("27:---------",j,i)
                if(j-i==1):
                    print("29---------")
                    print(pivotstart,pivotend)
                    pivotstart=pivotstart+1
                    pivotend=pivotend-1
                    print(pivotstart,pivotend)
                    i=pivotstart
                    j=pivotend

            else:
                print("in else")
                i=i+1
                flag=0
                print("--------->",pivotstart,pivotend)
                if(pivotend-pivotstart!=1 and j-i==1):
                    print("************************")
                    pivotstart=pivotstart+1
                    pivotend=pivotend-1
                    print(pivotstart,pivotend)
                    i=pivotstart
                    j=pivotend

        # i=0
        # while(i<j and flag==1):
        #     print("in the second")
        #     print(i,s[i:j],s[j:i-1:-1])
        #     if(s[i:j]==s[j:i-1:-1]):
        #         print("matched",s[i:j])
        #         len2 = len(s[i:j])
        #         break
        #     else:
        #         j=j-1
        #         flag=0
print("-------------->",max(len1,len2))