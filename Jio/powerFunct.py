# def powerMatch(num):
#     digits
#
#
# a = powerMatch(153)

a=150
total = 0
digit = len(str(a))
for item in str(a):
    total = total + pow(int(item),digit)
print(total)