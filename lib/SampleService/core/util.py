'''
Contains various miscellaneous utilies such as argument checkers.
'''


def not_falsy(item, item_name: str):
    '''
    Check if a value is falsy and throw and exception if so.
    :param item: the item to check for falsiness.
    :param item_name: the name of the item to include in any exception.
    :raises ValueError: if the item is falsy.
    :returns: the item.
    '''
    if not item:
        raise ValueError(f'{item_name} cannot be a value that evaluates to false')
    return item
