class Solution:
    def func(self,s,t):
        dic = {}
        flag = False
        for index, word in enumerate(s):
            if word not in dic:
                dic[word] = t[index]
            if word in dic and dic[word] == t[index]:
                flag = True
            else:
                flag = False
                break
        print(dic)
        return flag

    def isIsomorphic(self, s: str, t: str) -> bool:
        sol = Solution()
        flag1 = sol.func(s,t)
        flag2 = sol.func(t,s)
        if(flag1 and flag2):
            return True
        else:
            return False
s1 = Solution()
p = s1.isIsomorphic("add","egg")
print(p)