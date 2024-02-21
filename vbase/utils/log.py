"""
Default logging interface
"""

import logging


_LOG_FORMATTER = logging.Formatter(
    "[%(asctime)s][%(threadName)s][%(levelname)s]:%(message)s"
)


def get_default_logger(name: str) -> logging.Logger:
    """
    Get default logger for a given name.

    :param name: The logger name.
    :return: The logger object.
    """

    # Set Null log handler to avoid "No handlers could be found for logger XXX".
    # This is important for library code, which may contain code to log events
    # if a user of the library does not configure logging.
    if len(logging.getLogger().handlers) == 0:
        logging.getLogger().addHandler(logging.NullHandler())

    log = logging.getLogger(name)
    log.setLevel(logging.INFO)

    # Add a handler for the log if one isn't present.
    if len(log.handlers) == 0:
        handler = logging.StreamHandler()
        handler.setFormatter(_LOG_FORMATTER)
        log.addHandler(handler)
        log.propagate = False

    return log
