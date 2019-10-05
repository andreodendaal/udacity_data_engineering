"""
Prototype: Strip values out of a .sas file into dictionary for look-up usage


"""

import re

f = open('I94_SAS_Labels_Descriptions.SAS')
country_codes = {}
line = f.readline()

while line:
    if re.search("I94CIT & I94RES", line):
        print("I94CIT & I94RES")
    # match = re.search("/\*", line)
    match = re.search("I94CIT & I94RES", line)
    if match:
        print(line)
        line = f.readline()

        match = re.search("I94CIT & I94RES", line)

        while match is None:
            if line != '\n':
                if line.find('=') != -1:
                    line = line.replace("'", '')
                    line_split = line.split('=')
                    l_key = line_split[0].strip()
                    l_val = line_split[1].lstrip().replace('\n', '')
                    country_codes[l_key] = l_val


            line = f.readline()
            match = re.search("/\*", line)
            #match = re.search("I94CIT & I94RES", line)

    line = f.readline()

print(country_codes)

f.close()