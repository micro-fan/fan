from fan.utils import _make_key


def test_make_key():
    class A:
        pass
    args = (1, ('bla', ))
    kwargs = {'a': A()}

    key1 = _make_key(args, kwargs)
    key2 = _make_key(args, kwargs)
    assert key1 == key2, 'Keys must be the same'
