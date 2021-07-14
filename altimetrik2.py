lst = [[10, 2, 13], [41, 15, 6], [71], 8, 91]
lst2 = []
for i in lst:
    if(isinstance(i,list)):
        for j in i:
            print("-----",j)
            lst2.append(j)
    else:
        print("*****",i)
        lst2.append(i)
print(lst2)



# 1,Arvindh,40000,Hyderabad$AP$800042
# 2,Bala,30000,Chennai$TamilNadu$600042


# create external table state(id int,name varchar(20),salary int, details ArrayType(statename varchar(20,code varchar(10),pin int)))
# row format delimited
# fields terminated by ","
