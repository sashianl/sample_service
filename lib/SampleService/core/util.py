
# TODO docs
# TODO test


def not_falsy(item, item_name: str):
    if not item:
        raise ValueError(f'{item_name} cannot be a value that evaluates to false')
    return item
