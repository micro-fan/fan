def _make_key(args, kwargs):
    key = args
    if kwargs:
        key += (object(), )
        for item in kwargs.items():
            key += item

    return key


def async_cache(fun):
    cache = {}

    async def _inner(*args, **kwargs):
        key = _make_key(args, kwargs)
        if key not in cache:
            result = await fun(*args, **kwargs)
            cache[key] = result

        return cache[key]

    return _inner
