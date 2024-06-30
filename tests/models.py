import random
from dataclasses import dataclass
import datetime


@dataclass
class Car:
    Name: str = "foo"
    Year: datetime.date = datetime.date(1000, 1, 1)
    Miles_per_Gallon: int = 0
    Horsepower: int = 0


@dataclass
class Transaction:
    customer: str = None
    price: int = None
    date: datetime.date = datetime.date.today()
    is_deleted: bool = False
    pk: int = random.randint


@dataclass
class CDCTransaction:
    pk: int
    op: str
    offset: int
    fullDoc: Transaction = None
