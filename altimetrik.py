lst = [2, 11, 7, 10]
result = 12
ans = []
for i in range(0,len(lst)):
    if(result-lst[i] in lst):
        ans.append(i)
        ans.append(lst.index(result-lst[i]))
        break
print(ans)