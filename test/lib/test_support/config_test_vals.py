def s(d):
    return dict(sorted(d.items()))


def val1(d1):
    def f(key, d2):
        return f'1, {key}, {s(d1)}, {s(d2)}'
    return f


def val2(d1):
    def f(key, d2):
        return f'2, {key}, {s(d1)}, {s(d2)}'
    return f


def fail_val(d):
    raise ValueError("we've no functions 'ere")


def pval1(d1):
    def f(prefix, key, d2):
        return f'1, {prefix}, {key}, {s(d1)}, {s(d2)}'
    return f


def pval2(d1):
    def f(prefix, key, d2):
        return f'2, {prefix}, {key}, {s(d1)}, {s(d2)}'
    return f


def fail_prefix_val(d):
    raise ValueError("we've no prefix functions 'ere")


def prefix_validator_test_builder(cfg):
    arg = cfg['fail_on_arg']

    def val(prefix, key, args):
        if arg in args:
            return f'{prefix}, {key}, {dict(sorted(args.items()))}'
    return val
