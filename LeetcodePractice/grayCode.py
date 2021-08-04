#def grayCode(self, n: int) -> List[int]:
ls = []
ans = []

cnt = 0
pivot = 0

class Solution:
    def grayCode(self, n: int):
        global cnt
        global pivot

        for i in range(1, n + 1):
            ls.append(pow(2, i))
        print(ls)
        latest = [0 for i in range(0, n)]
        print(latest)

        def complement(latest, i):
            global cnt
            latest[pivot] = 1
            ans.append(latest[:])
            cnt = cnt + 1
            traverse = [i for i in range(0, pivot)]
            j = 0
            print(traverse)
            print("19----------------", latest, cnt, i)
            while (cnt < i):
                if (j < len(traverse)):
                    if (latest[j] == 0):
                        latest[j] = 1
                        ans.append(latest[:])
                    else:
                        latest[j] = 0
                        ans.append(latest[:])
                    j = j + 1
                    cnt = cnt + 1
                    if (cnt < i and len(traverse) - j == 1):
                        j = 0

        ans.append(latest[:])
        cnt = cnt + 1

        for i in ls:
            print("ans,cnt,i:::::::::::::", ans, cnt, i)
            if (cnt < i):
                complement(latest, i)
            pivot = pivot + 1
            print("--------", pivot, i)
        print("----------------->", ans)
        final_ans = []
        for lst in ans:
            sum = 0
            cnt2 = n
            for i in range(0, len(lst)):
                sum = sum + lst[i] * pow(2, cnt2 - 1)
                cnt2 = cnt2 - 1
            final_ans.append(sum)
        return final_ans
s = Solution()
ans1 =s.grayCode(10)
print(ans1)
