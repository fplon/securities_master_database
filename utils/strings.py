def get_snake_case_from_camel_case(name: str) -> str: 
    """Camel case to snake case, eg. 'OneTwo' returns 'one_two'.
        Covers strings that have consecutive capital letters
        as well

    Args:
        name (str): camel case input string

    Returns:
        str: snake case string
    """

    new_chars = []
    for i, char in enumerate(name): 
        if i == len(name)-1 or i == 0: 
            new_chars.append(char)
        elif char.isupper() and name[i+1].islower():
            new_chars.append('_')
            new_chars.append(char)
        elif char.islower() and name[i+1].isupper(): 
            new_chars.append(char)
            new_chars.append('_')
        else: 
            new_chars.append(char)

    new_name = ''.join(new_chars)
    return new_name.lower().replace('__', '_')
   

def get_sql_string_from_file(path: str) -> str: 
    """Reads a .sql file and returns the text as a string.

    Args:
        path (str): path where .sql file is stored. 

    Returns:
        str: SQL string
    """

    with open(path, 'r') as r:
        sql =  r.read()
    return sql.replace('\n', '').replace('\t', '')
