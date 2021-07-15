# nums = [3,3]
# target = 6
nums= [2,11,15,7]
target = 9
# nums = [3,2,4]
# target = 6
# # for i in range(0,len(nums)):
# #     for j in range(i+1,len(nums)):
# #         if(nums[i]+nums[j]==target):
# #             print(i,j)
# print(nums[1:])
# for i in range(0,len(nums)):
#     if(target-nums[i] in nums[i+1:]):
#         print("-----------",nums.index(nums[i]),nums.index(target-nums[i],i+1))

# dc = dict()
#
# for i in range(0,len(nums)):
#     for j in range(i+1,len(nums)):
#         if(nums[i]+nums[j]==target):
#             print(i,j)
# print(nums[1:])
# for i in range(0,len(nums)):
#     if(target-nums[i] in nums[i+1:]):
#         print("-----------",nums.index(nums[i]),nums.index(target-nums[i],i+1))
#
# ans=[]
# value2=None
# index2 =None
# for index,value in enumerate (nums):
#     if(target-value  in nums[index+1:]):
#         value2=value
#         print(index,value)
#         ans.append(index)
#         break
# print(nums[ans[0]+1:])
# for i in range(ans[0]+1,len(nums)):
#     if(target-value2 ==nums[i]):
#         ans.append(i)
#         break
# print(ans)
dc = {}
for index,value in enumerate(nums):
    sec = target-value
    if sec in dc:
        print(dc[target-value],index)
        break
    dc[value]=index
    print("dc after : ",dc)

print(dc)
