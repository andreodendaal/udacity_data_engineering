import re


def proc_sasref(lookup_p):
    """
    Strip values out of a .sas reference file (I94_SAS_Labels_Descriptions.SAS) into dictionary for look-up usage


    Extended description of function.

    Parameters:
    lookup_p (string): Description of arg1

    Returns:
    int: Description of return value

    """
    return_set = {}
    lookup_lst = ['i94port', 'i94mode', 'i94addr', 'i94visa']

    if lookup_p == 'i94cit' or lookup_p == 'i94res':
        search_str = "I94CIT & I94RES"
    elif lookup_p in lookup_lst:
        search_str = lookup_p.upper()
    elif lookup_p == 'headers':
        search_str = "/\*"
    else:
        return None

    f = open('I94_SAS_Labels_Descriptions.SAS')
    line = f.readline()

    while line:
        if re.search(search_str, line):

            # process lookup files
            if lookup_p != 'headers':
                line = f.readline()
                match = re.search(search_str, line)
                while match is None:
                    if line != '\n':
                        if line.find('=') != -1:
                            line_split = line.split('=')
                            l_key = (re.sub(r"[*()\"\'#/@;:<>{}`+=~|.!?,\n]", "", line_split[0])).strip()
                            l_val = re.sub(r"[*()\"\'#/@;:<>{}`+=~|.!?,\n]", "", line_split[1]).strip()
                            return_set[l_key] = l_val

                    line = f.readline()
                    match = re.search("/\*", line)
                return return_set

            elif lookup_p == 'headers':
                # process headers only
                line_split = line.split('-')
                if len(line_split) == 2:
                    l_key = (re.sub(r"[*()\"#/@;:<>{}`+=~|.!?,\n]", "", line_split[0])).strip()
                    l_val = re.sub(r"[*()\"#/@;:<>{}`+=~|.!?,\n]", "", line_split[1]).strip()
                    return_set[l_key] = l_val
                else:
                    return_set[line] = '0'



        line = f.readline()
    f.close()
    return return_set

if __name__ == '__main__':
    print(proc_sasref('i94port'))
