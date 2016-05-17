from __future__ import absolute_import
class Index(object):
    @staticmethod
    def encode(index, max_len):
        index_str = str(index)
        prefix = '0' * (max_len - len(index_str))
        return prefix + index_str

    @staticmethod
    def digits(x):
        if isinstance(x, float):
            x = int(x)
        if (x < 10):
            return 1
        else:
            return 1+Index.digits(x/10)

