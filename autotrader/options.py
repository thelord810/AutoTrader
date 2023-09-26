import numpy as np
import pandas as pd
import pendulum
from finta import TA
from typing import Union
from autotrader_custom_repo.AutoTrader.autotrader import expiry_calculator
from datetime import datetime
import requests,json


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


def getOptionPrice(
    strike: int,
    type: str = "CE"
) -> float:
    """Returns Price of Instrument

    Parameters
    ----------
    strike : int
        option Stike
    type : str
        Option Type: CE or PE
    expiry : str
        Expiry of option
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

def getStrikeByPrice(
    instrument: str,
    price: float,
    right: str = "call",
    expiry: str = "None",
    strike: int = "",
    exchange: str = "NFO",
    product: str = "options",

) -> float:
    """Returns Strike of Instrument around a price range

    Parameters
    ----------
    price : float
        option Price for which strike is to be fetched
    type : str
        Option Type: CE or PE
    expiry : str
        Expiry of option
    Returns
    -------
    ATMStrike : int
        Returns ATM Strike price

    References
    ----------
    https://www.tradingview.com/script/r6dAP7yi/
    """

    api_url = "http://127.0.0.1:8000/optionchain"


    info = {
        "instrument": instrument,
        "exchange": exchange,
        "strike": strike,
        "product": product,
        "right": right,
        "expiry": expiry
    }
    response = requests.post(api_url, json=info)
    json_response = json.loads(response.content)
    df = pd.DataFrame(json_response['Success'])
    exactmatch = df[df['ltp'] == price]
    if not exactmatch.empty:
        strike = exactmatch.strike_price
    else:
        lowerneighbour_ind = df[df['ltp'] < price]['ltp'].idxmax()
        upperneighbour_ind = df[df['ltp'] > price]['ltp'].idxmin()
        if(right == "call"):
            nearest = df.iloc[upperneighbour_ind]
        else:
            nearest = df.iloc[lowerneighbour_ind]
        strike = nearest.strike_price
    return int(strike)


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

def getExpiryDate(expiryType, Contract):
    if(expiryType == "Weekly" and Contract == "Current"):
        expiryDate_object = expiry_calculator.getNearestWeeklyExpiryDate()

    expiryDate = expiryDate_object.strftime("%d%b%y").upper()
    return expiryDate

def getExpiryDateIcici(expiryType, Contract):
    if(expiryType == "Weekly" and Contract == "Current"):
        expiryDate_object = expiry_calculator.getNearestWeeklyExpiryDate()

    expiryDate = expiryDate_object.strftime("%Y-%m-%d")
    expireyStamp = f"{expiryDate}T06:00:00.000Z"
    return expireyStamp

