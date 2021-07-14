ls = ["bala", "john", "alba", "bata", "laba",  "nhoj"]

#[["bala", "alba", "laba"],["john","nhoj"],["bata"]]
ls.sort()
ls2 =ls

ls2 = [''.join(sorted(i)) for i in ls2]
print(ls,ls2)
final = []
completed = []

for i in range(0,len(ls2)):
    j=i+1
    a= []
    currWord = ls2[i]
    while(j<len(ls2)):
        if(ls2[j]==currWord and ls2[j] not in completed):
            a.append(ls[j])
        j=j+1
    if(len(a)<=1 and ls2[i]  in completed):
        pass
    else:
        a.append(ls[i])
        final.append(a)
        completed.append(currWord)
print(completed)
print("final------",final)