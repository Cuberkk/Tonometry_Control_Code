"""Numeric display formatting shared by the preprocessing and fitting notebooks."""
from decimal import Decimal


def format_fractional_digits(value, position=None):
    """Keep two significant digits in the fractional part for tables and ticks."""
    text = format(Decimal(str(value)), 'f')
    # Recompute precision when rounding moves the first nonzero decimal digit.
    for _ in range(2):
        fraction = text.partition('.')[2]
        places = 2 + len(fraction) - len(fraction.lstrip('0')) if fraction.strip('0') else 2
        text = f'{value:.{places}f}'
    return text
