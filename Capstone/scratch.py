
def dict_gen(name):

    dict_names = ['port', 'country']
    name = {}
    for ctr, val in enumerate(dict_names):
        name[ctr] = val
    return name

print(dict_gen('dict_name'))