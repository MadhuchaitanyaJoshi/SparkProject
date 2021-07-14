import re
class Solution:
    def reverse(self, x: int) -> int:
        print(x)
        posit = False
        maxl = pow(2,31) - 1
        minl = -1*(pow(2, 31))
        print(maxl,minl)
        if (x >= minl and x <= maxl):

            if (x >= 0):
                s = str(x)[::-1]
                print("here in 0")
                posit = True
                return int(s)
            else:
                s = str(x)
                print("1---", s)
                s = s[1:]
                s = s[::-1]
                print("2-----", s)
                posit = False

                i = 0
                while (s[i] == '0' and i<len(s)):
                    i = i + 1
                return int(s[i:]) if posit == True else (-1 * int(s[i:]))
        else:
            print("here")
            return 0


s = Solution()
k = s.reverse(1534236469)
print(k)
