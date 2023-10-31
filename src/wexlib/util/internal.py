def str_to_bool(string):
    if string in ['true', 'True', 'TRUE', 't', 'T', 'yes', 'Yes', 'YES', 'y', 'Y']:
        return True
    if string in ['false', 'False', 'FALSE', 'f', 'F', 'no', 'No', 'NO', 'n', 'N']:
        return False
    else:
        return False #fallback to false