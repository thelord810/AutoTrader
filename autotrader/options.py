import numpy as np
import pandas as pd
import pendulum
from finta import TA
from typing import Union
from autotrader_custom_repo.AutoTrader.autotrader import expiry_calculator


def getATMStrikePrice(
    data: pd.DataFrame,
    nearestMultiple: int = 50
) -> int:
    """Returns ATM Strike Price of Instrument

    Parameters
    ----------
    data : pd.DataFrame
        The OHLC data.
    nearestMultiple : int, optional
        Difference between 2 nearest strikes for Instrument. The default is 50.

    Returns
    -------
    ATMStrike : int
        Returns ATM Strike price

    References
    ----------
    https://www.tradingview.com/script/r6dAP7yi/
    """

    lastPrice = int(data.Close)
    remainder = int(lastPrice % nearestMultiple)
    if remainder < int(nearestMultiple / 2):
        return lastPrice - remainder
    else:
        return lastPrice + (nearestMultiple - remainder)


def isTodayWeeklyExpiryDay():
    expiryDate = expiry_calculator.getNearestWeeklyExpiryDate()
    todayDate = pendulum.today()
    if expiryDate == todayDate:
      return True
    return False


def isTodayOneDayBeforeWeeklyExpiryDay():
    expiryDate = expiry_calculator.getNearestWeeklyExpiryDate()
    todayDate = pendulum.today()
    if expiryDate - todayDate == 1:
      return True
    return False

