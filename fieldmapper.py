# field mapper
import re


def readfiels():
    with open('fields.txt') as f:
        data = f.readlines()

    data = [k.replace('\n', '') for k in data]
    # newdata = [('f{k}' + re.sub('\(|\s|\/.*|\)|\%|\Â°', '', k) for k in data]
    # newdata =  [f'{k[:-1]}' + ':' + re.sub('\(|\s|\/.*|\)|\%|\Â°', '', k) for k in data]
    dbfields = [re.sub('\(|\s|\/.*|\)|\%|\Â°', '', k) for k in data]
    foo = [k for k in zip(dbfields, data)]
    print(foo)
