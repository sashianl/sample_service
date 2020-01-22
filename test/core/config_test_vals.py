def val1(d1):
    def f(d2):
        return (1, d1, d2)
    return f


def val2(d1):
    def f(d2):
        return (2, d1, d2)
    return f


def fail_val(d):
    raise ValueError("we've no functions 'ere")
