import parser as parse
from funcy import chunks


key = "SomeKey"
a = parse.parser(key)
last_element = a.get_last_element()
elements = [num for num in range(1,int(last_element),50)]
result = list(chunks(70, elements))
count = 0
for el in result:
    a.run(el)
    count += 3500
    print(f'В файле {count} записей')








