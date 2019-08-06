import decimal
import random


def generate_decimal(div=10000) -> decimal.Decimal:
    return decimal.Decimal(random.randrange(10000))/div
