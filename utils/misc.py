def try_cast_to_int(x, logger=None):
    try:
        x = int(x)
        if not logger is None: logger.debug('point id interpreted as external id')
    except ValueError:
        if not logger is None: logger.debug('point id interpreted as name')
    finally:
        return x