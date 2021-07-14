base=1
fact=1
def recfact(num):
    global base,fact
    print(base)
    while(base<=num):
        fact=fact*base
        base=base+1
        recfact(num)
    return fact
a = recfact(4)
print("------------",a)
