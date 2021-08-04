# you can write to stdout for debugging purposes, e.g.
# print("this is a debug message")
import math
def solution(A, B, K):
    # write your code in Python 3.6
    val=0
    i=0
    for i in range(A,B):
        print("--------", A, K)
        if(i%K==0):
            print("*****",i,K)
            break
    if(i>0):
        return (math.ceil((B-i)/K)+(1 if math.ceil(B%K)==0 else 0))
    else:
        return 0+math.ceil((B-A)/K)+(1 if math.ceil(B%K)==0 else 0)
d = solution(0,14,2)
print(d)