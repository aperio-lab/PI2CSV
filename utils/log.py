import logging


def logger_class(obj: object) -> logging.Logger:
    return get_logger().getChild(type(obj).__name__)


def get_resource(filename, package='connectors'):
    import pkg_resources
    return pkg_resources.resource_filename(package, filename)


def get_logger(*args, **kwargs):
    from logging import getLogger
    return getLogger(*args, **kwargs)
