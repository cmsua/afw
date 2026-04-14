"""
Utilities for CLI tools
"""

import logging
import re
import unicodedata

# Stolen from https://github.com/django/django/blob/master/django/utils/text.py
def slugify(value):
    """
    Convert to ASCII. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.
    """
    value = str(value)
    value = (
        unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    )
    value = re.sub(r"[^\w\s-]", "", value.lower())
    return re.sub(r"[-\s]+", "-", value).strip("-_")


# LOGGING
class Formatter(logging.Formatter):
    """
    A formatter for output logging
    """

    grey = "\x1b[37m"
    yellow = "\x1b[33m"
    red = "\x1b[31m"
    bold_red = "\x1b[1;31m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(name)-10s - %(levelname)-7s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: logging.Formatter(grey + format + reset),
        logging.INFO: logging.Formatter(format),
        logging.WARNING: logging.Formatter(yellow + format + reset),
        logging.ERROR: logging.Formatter(red + format + reset),
        logging.CRITICAL: logging.Formatter(bold_red + format + reset),
    }

    def format(self, record):
        return self.FORMATS.get(record.levelno).format(record)


def setup_logging(debug: bool = False):
    """
    Sets up a custom formatter for output logs

    Parameters:
        debug (bool, default False): Whether to set a logging level of logging.DEBUG
    """
    ch = logging.StreamHandler()
    ch.setFormatter(Formatter())

    logging.basicConfig(
        handlers=[ch],
        level=logging.DEBUG if debug else logging.INFO,
    )