import re
str="10.102.001.12"
pattern = re.compile("[A-Za-z]+")
if pattern.fullmatch(str) is None:
    print("mathhed")
else:
    print("not")