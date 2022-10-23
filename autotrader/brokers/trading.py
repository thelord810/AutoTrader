from __future__ import annotations

import json

import numpy as np
from datetime import datetime

import requests

from autotrader_custom_repo.AutoTrader.autotrader.brokers.broker_utils import BrokerUtils
from autotrader_custom_repo.AutoTrader.autotrader import options
import logging
import pytz
from pgcopy import CopyManager
import config, psycopg2


class Order:
    """AutoTrader Order object.

    Attributes
    ----------
    instrument : str
        The trading instrument of the order.
    direction : int
        The direction of the order (1 for long, -1 for short).
    order_type : str
        The type of order. The default is 'market'.
    size : float
        The number of units.
    base_size : float
         The number of units, in the base currency (pre-HCF conversion).
    target_value : float
        The target value of the resulting trade, specified in the home
        currency of the account.
    order_limit_price : float
        The limit price of the order (for 'limit' and 'stop-limit' order
        types).
    order_stop_price : float
        The stop price of the order (for 'stop-limit' order types).
    order_price : float
        The price of the instrument when the order was placed.
    order_time : datetime
        The time at which the order was placed.
    stop_loss : float
        The price to set the stop-loss at.
    stop_distance : float
        The pip distance between the order price and the stop-loss.
    stop_type : str
        The type of stop-loss (limit or trailing). The default is 'limit'.
    take_profit : float
        The price to set the take-profit at.
    take_distance : float
        The pip distance between the order price and the take-profit.
    related_orders : list
        A list of related order/trade ID's.
    id : int
        The order ID.
    pip_value : float, optional
        The pip value of the product being traded. Specify this for non-FX
        products when using stop_distance/take_distance arguments. The default
        is None.
    currency : str
        The base currency of the order (IB only).
    secType : str
        The security type (IB only).
    contract_month : str
        The contract month string (IB only).
    localSymbol : str
        The exchange-specific instrument symbol (IB only).
    post_only : bool, optional
        Enforce that the order is placed as a maker order (dYdX only). The
        default is False.
    limit_fee : str, optional
        The maximum fee to accept as a percentage (dYdX only). The default
        is '0.015'.
    broker : str
        The broker to which the order should be submitted.
    ccxt_params : dict, optional
        The CCXT parameters dictionary to pass when creating an order. The
        default is {}.
    """

    def __init__(
        self,
        instrument: str = None,
        direction: int = None,
        order_type: str = "market",
        size: float = None,
        order_limit_price: float = None,
        order_stop_price: float = None,
        stop_loss: float = None,
        stop_type: str = None,
        take_profit: float = None,
        **kwargs,
    ) -> Order:

        # establish database connection
        self.conn = psycopg2.connect(database="Orders",
                                host="localhost",
                                user="postgres",
                                password="password",
                                port=5432)

        # column names in the database (pgcopy needs it as a parameter)
        self.COLUMNS = ('time', 'symbol', 'order_type', 'size', 'order_price', 'order_time', 'stop_loss', 'stop_type', 'direction')

        # create a copy manager instance
        self.mgr = CopyManager(self.conn, 'order', self.COLUMNS)
        # Required attributes
        self.instrument = instrument
        self.direction = direction
        self.order_type = order_type

        # Optional arguments
        self.size = size
        self.base_size = None
        self.target_value = None
        self.order_price = None
        self.order_time = None
        self.order_limit_price = order_limit_price
        self.order_stop_price = order_stop_price
        self.pip_value = None
        self.HCF = 1

        # Precision
        self.price_precision = 5
        self.size_precision = 5

        # Multi-exchange handling
        self.broker = None

        # Stop loss arguments
        self.stop_type = stop_type
        self.stop_loss = stop_loss
        self.stop_distance = None

        # Take profit arguments
        self.take_profit = take_profit
        self.take_distance = None

        self.related_orders = None

        self.data_name = None  # When using custom DataStream

        # IB attributes
        self.currency = None
        self.secType = None
        self.contract_month = None
        self.localSymbol = None

        # Oanda attributes
        self.trigger_price = "DEFAULT"

        # dydx attributes
        self.post_only = False
        self.limit_fee = "0.015"

        # CCXT attributes
        self.ccxt_params = {}

        self.reason = None

        self.strategy = None
        self.granularity = None
        self._sizing = None
        self._risk_pc = None

        # Meta-data
        self.id = None
        self.status = None  # options: pending -> open -> cancelled | filled

        # Unpack kwargs
        for item in kwargs:
            setattr(self, item, kwargs[item])

        # Enforce stop type
        if self.stop_loss is not None or self.stop_distance is not None:
            self.stop_type = self.stop_type if self.stop_type is not None else "limit"

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        if self.instrument is None:
            # Blank order
            return "Blank order"

        else:
            # Order constructed with instrument
            if self.size is not None:
                if self.direction is None:
                    return "Invalid order (direction not specified)"

                side = "buy" if self.direction > 0 else "sell"

                string = (
                    f"{round(self.size, self.size_precision)} "
                    + f"unit {self.instrument} {self.order_type} "
                    + f"{side} order"
                )

                # Append additional information
                if self.order_type == "limit":
                    if self.order_limit_price is None:
                        return "Invalid order (limit price not provided)"
                    string += f" @ {self.order_limit_price}"

                elif self.order_type == "stop-limit":
                    if self.order_limit_price is None:
                        return "Invalid order (limit price not provided)"
                    elif self.order_stop_price is None:
                        return "Invalid order (stop price not provided)"
                    string += f" @ {self.order_stop_price} / {self.order_limit_price}"

                return string

            else:
                # Size un-assigned
                return f"{self.instrument} {self.order_type} Order"

    def __call__(
        self,
        broker=None,
        order_price: float = None,
        order_time: datetime = datetime.now(),
        HCF: float = None,
        precision: dict = None,
    ) -> None:
        """Order object, called before submission to broker in
        autobot._qualify_orders.

        Parameters
        ----------
        broker : AutoTrader broker API instance, optional
            The broker-autotrader api instance. The default is None.
        order_price : float, optional
            The order price. The default is None.
        order_time : datetime, optional
            The time of the order. The default is datetime.now().
        HCF : float, optional
            The home conversion factor. The default is 1.
        precision : dict, optional
            A dictionary containing the precision for order size and price.
            The default is None.

        Returns
        -------
        None
            Calling an Order will ensure all information is present.
        """
        self.order_price = order_price if order_price else self.order_price
        utc = pytz.UTC
        self.order_time = utc.localize(datetime.now())
        #self.order_time = order_time if order_time else self.order_time
        self.HCF = HCF if HCF is not None else self.HCF

        # Assign precisions
        if precision is not None:
            self.price_precision = precision["price"]
            self.size_precision = precision["size"]

        # Enforce size scalar
        self.size = abs(self.size) if self.size is not None else self.size

        if self.order_type not in ["close", "modify"]:
            self._set_working_price()
            self._calculate_exit_prices(broker)
            self._calculate_position_size(broker)

        self.status = "submitted"
        self.submitted = True

    def _set_working_price(self, order_price: float = None) -> None:
        """Sets the Orders' working price, for calculating exit targets.

        Parameters
        ----------
        order_price : float, optional
            The order price.

        Returns
        -------
        None
            The working price will be saved as a class attribute.
        """
        order_price = order_price if order_price is not None else self.order_price
        if self.order_type == "limit" or self.order_type == "stop-limit":
            self._working_price = round(self.order_limit_price, self.price_precision)
        else:
            if order_price is not None:
                self._working_price = round(order_price, self.price_precision)
            else:
                self._working_price = None

    def _calculate_exit_prices(self, broker=None, working_price: float = None) -> None:
        """Calculates the prices of the exit targets from the pip distance
        values.

        Parameters
        ----------
        broker : AutoTrader Broker Interface, optional
            The autotrade-broker instance. The default is None.
        working_price : float, optional
            The working price used to calculate amount risked. The default is
            None.

        Returns
        -------
        None
            The exit prices will be assigned to the order instance.
        """
        working_price = (
            round(working_price, self.price_precision)
            if working_price is not None
            else self._working_price
        )

        if broker is None:
            # No broker provided, create nominal utils instance
            utils = BrokerUtils()
        else:
            # Use broker-specific utilities
            utils = broker._utils

        pip_value = (
            self.pip_value
            if self.pip_value is not None
            else utils.get_pip_ratio(self.instrument)
        )

        # Calculate stop loss price
        if self.stop_loss is None and self.stop_distance is not None:
            # Stop loss provided as pip distance, convert to price
            stop_loss = (
                working_price - np.sign(self.direction) * self.stop_distance * pip_value
            )
            self.stop_loss = round(stop_loss, self.price_precision)

        if (
            self.stop_type == "trailing"
            and self.stop_distance is None
            and working_price is not None
        ):
            # Convert stop_loss price to stop_distance as well
            self.stop_distance = (
                np.sign(self.direction) * (working_price - self.stop_loss) / pip_value
            )

        # Calculate take profit price
        if self.take_profit is None and self.take_distance is not None:
            # Take profit pip distance specified, convert to price
            take_profit = (
                working_price + np.sign(self.direction) * self.take_distance * pip_value
            )
            self.take_profit = round(take_profit, self.price_precision)

    def _calculate_position_size(
        self,
        broker=None,
        working_price: float = None,
        HCF: float = 1,
        risk_pc: float = 0,
        sizing: str | float = "risk",
        amount_risked: float = None,
    ) -> None:
        """Calculates trade size for order.

        Parameters
        ----------
        broker : AutoTrader Broker Interface, optional
            The autotrade-broker instance. The default is None.
        working_price : float, optional
            The working price used to calculate amount risked. The default is None.
        HCF : float, optional
            The home conversion factor. The default is 1.
        risk_pc : float, optional
            The percentage of the account NAV to risk on the trade. The default is 0.
        sizing : str | float, optional
            The sizing option. The default is 'risk'.
        amount_risked : float, optional
            The dollar amount risked on the trade. The default is None.

        Returns
        -------
        None
            The trade size will be assigned to the order instance.
        """
        working_price = (
            working_price if working_price is not None else self._working_price
        )
        HCF = self.HCF if self.HCF is not None else HCF
        sizing = self._sizing if self._sizing is not None else sizing
        risk_pc = self._risk_pc if self._risk_pc is not None else risk_pc

        if self.size is None:
            # Size has not been set
            if self.target_value is not None:
                # Calculate size based on target trade value
                self.size = round(
                    self.target_value / working_price / HCF, self.size_precision
                )
            elif self.base_size is not None:
                # Size provided in base units, need to convert it
                self.size = round(self.base_size / HCF, self.size_precision)
            else:
                # Size not provided, need to calculate it
                if sizing == "risk":
                    # Calculate size from SL placement
                    try:
                        amount_risked = (
                            amount_risked
                            if amount_risked
                            else broker.get_NAV() * risk_pc / 100
                        )

                        size = broker._utils.get_size(
                            instrument=self.instrument,
                            amount_risked=amount_risked,
                            price=working_price,
                            HCF=HCF,
                            stop_price=self.stop_loss,
                            stop_distance=self.stop_distance,
                        )
                        self.size = round(size, self.size_precision)

                    except AttributeError:
                        # Broker is None type
                        raise Exception(
                            "Cannot calculate size without broker access. Please "
                            + "pass size argument, or add broker to Order object."
                        )

                else:
                    # Use position size provided via sizing key
                    self.size = sizing

        else:
            # Size has been set, enforce precision
            self.size = round(self.size, self.size_precision)

    @classmethod
    def _partial_fill(cls, order: Order, units_filled: float) -> Order:
        """Partially fills the order."""
        # Instantiate new order
        order_to_be_filled = cls()

        # Inherit attributes from base order
        for attribute, value in order.__dict__.items():
            setattr(order_to_be_filled, attribute, value)

        # Reset ID
        order_to_be_filled.id = None

        # Transfer units
        order_to_be_filled.size = units_filled
        order.size -= units_filled

        return order_to_be_filled

    def _check_precision(
        self,
    ):
        # TODO - implement
        raise NotImplementedError("This method has not been implemented yet.")

    def _validate(
        self,
    ):
        # TODO - add order validation method, ie. for IB, check all attributes are
        # assigned (eg. sectype, etc)
        raise NotImplementedError("This method has not been implemented yet.")

    def as_dict(self) -> dict:
        """Converts Order object to dictionary.

        Returns
        -------
        dict
            The order instance returned as a dict object.

        Notes
        -----
        This method enables legacy code operation, returning order/trade
        objects as a dictionary.
        """
        return self.__dict__

    @classmethod
    def _from_dict(cls, order_dict: dict) -> Order:
        return Order(**order_dict)


class MarketOrder(Order):
    """Market order type."""

    def __init__(
        self,
        instrument: str = None,
        direction: int = None,
        size: float = None,
        **kwargs,
    ):
        # Create base Order
        super().__init__(
            instrument=instrument,
            direction=direction,
            order_type="market",
            size=size,
            **kwargs,
        )


class LimitOrder(Order):
    """Limit order type."""

    def __init__(
        self,
        instrument: str = None,
        direction: int = None,
        size: float = None,
        order_limit_price: float = None,
        **kwargs,
    ):
        # Create base Order
        super().__init__(
            instrument=instrument,
            direction=direction,
            order_type="limit",
            size=size,
            order_limit_price=order_limit_price,
            **kwargs,
        )


class StopLimitOrder(Order):
    """Stop-limit order type."""

    def __init__(
        self,
        instrument: str = None,
        direction: int = None,
        size: float = None,
        order_limit_price: float = None,
        order_stop_price: float = None,
        **kwargs,
    ):
        # Create base Order
        super().__init__(
            instrument=instrument,
            direction=direction,
            order_type="stop-limit",
            size=size,
            order_limit_price=order_limit_price,
            order_stop_price=order_stop_price,
            **kwargs,
        )


class IsolatedPosition(Order):
    """AutoTrader IsolatedPosition. Use to connect SL and TP orders to individual
    trades.

    Attributes
    ----------
    unrealised_PL : float
        The floating PnL of the trade.
    margin_required : float
        The margin required to maintain the trade.
    time_filled : datetime
        The time at which the trade was filled.
    fill_price : float
        The price at which the trade was filled.
    last_price : float
        The last price observed for the instrument associated with the trade.
    last_time : datetime
        The last time observed for the instrument associated with the trade.
    exit_price : float
        The price at which the trade was closed.
    exit_time : datetime
        The time at which the trade was closed.
    fees : float
        The fees associated with the trade.
    parent_id : int
        The ID of the order which spawned the trade.
    id : int
        The trade ID.
    status : str
        The status of the trade (open or closed).
    split : bool
        If the trade has been split.

    Notes
    -----
    When a trade is created from an Order, the Order will be marked as filled.
    """

    def __init__(self, order: Order = None, **kwargs) -> IsolatedPosition:

        # Trade data
        self.unrealised_PL = 0
        self.margin_required = 0
        self.time_filled = None
        self.fill_price = None

        self.last_price = None
        self.last_time = None

        self.profit = 0
        self.balance = None
        self.exit_price = None
        self.exit_time = None
        self.fees = None

        # Meta data
        self.parent_id = None  # ID of order which spawned trade
        self.id = None
        self.status = None  # options: open -> closed
        self.split = False

        # Unpack kwargs
        for item in kwargs:
            setattr(self, item, kwargs[item])

        # Inherit order attributes
        if order:
            self._inheret_order(order)
            self.parent_id = order.id

    def __repr__(self):
        direction = "long" if self.direction > 0 else "short"
        return (
            f"{round(self.size,3)} unit {direction} {self.instrument} IsolatedPosition"
        )

    def __str__(self):
        return "AutoTrader IsolatedPosition"

    def _inheret_order(self, order: Order) -> None:
        for attribute, value in order.__dict__.items():
            setattr(self, attribute, value)

    @classmethod
    def _split(cls, trade: IsolatedPosition, split_units: float) -> IsolatedPosition:
        """Splits parent IsolatedPosition into new object for partial
        closures.

        split units are given to the new trade.
        """
        split_trade = cls()
        for attribute, value in trade.__dict__.items():
            setattr(split_trade, attribute, value)

        # Reset ID
        split_trade.parent_id = trade.parent_id
        split_trade.order_id = None

        # Transfer units
        split_trade.size = split_units
        trade.size -= split_units

        # Mark original trade as split
        trade.split = True

        return split_trade


class Trade:
    """AutoTrader Trade object. Represents an exchange of value."""

    def __init__(
        self,
        instrument: str,
        order_price: float,
        order_time: datetime,
        order_type: str,
        size: float,
        fill_time: datetime,
        fill_price: float,
        fill_direction: int,
        fee: float,
        **kwargs,
    ) -> Trade:
        """Trade constructor."""
        # Trade data
        self.fill_time = fill_time
        self.fill_price = fill_price
        self.direction = fill_direction
        self.fee = fee
        self.order_price = order_price
        self.order_time = order_time
        self.order_type = order_type
        self.size = size
        self.instrument = instrument

        # Meta-data
        self.id = None
        self.order_id = None

        for item in kwargs:
            setattr(self, item, kwargs[item])

    def __repr__(self):
        direction = "long" if self.direction > 0 else "short"
        return f"{round(self.size,3)} unit {direction} {self.instrument} trade @ {self.fill_price}"

    def __str__(self):
        return "AutoTrader Trade"


class Position:
    """AutoTrader Position object.

    Attributes
    ----------
    instrument : str
        The trade instrument of the position.
    pnl : float
        The pnl of the position.
    long_units : float
        The number of long units in the position.
    long_PL : float
        The PnL of the long units in the position.
    long_margin : float
        The margin required to maintain the long units in the position.
    short_units : float
        The number of short units in the position.
    short_PL : float
        The PnL of the short units in the position.
    short_margin : float
        The margin required to maintain the short units in the position.
    total_margin : float
        The total margin required to maintain the position.
    trade_IDs : list[int]
        The trade ID's associated with the position.
    net_position : float
        The total number of units in the position.
    net_exposure : float
        The net exposure (in $ value) of the position.
    PL : float
        The floating PnL (IB only).
    contracts : list
        The contracts associated with the position (IB only).
    portfolio_items : list
        The portfolio items associated with the position (IB only).
    """

    def __init__(self, **kwargs):
        self.instrument = None
        self.pnl = None
        self.long_units = None
        self.long_PL = None
        self.long_margin = None
        self.short_units = None
        self.short_PL = None
        self.short_margin = None
        self.total_margin = None
        self.trade_IDs = None
        self.net_position = None
        self.net_exposure = None

        # IB Attributes
        self.PL = None
        self.contracts = None
        self.portfolio_items = None

        # dYdX Attributes
        self.entry_price = None

        # CCXT Attributes
        self.ccxt = None

        for item in kwargs:
            setattr(self, item, kwargs[item])

    def __repr__(self):
        return f"Position in {self.instrument}"

    def __str__(self):
        return "AutoTrader Position"

    def as_dict(self) -> dict:
        """Converts Position object to dictionary.

        Returns
        -------
        dict
            The Position instance returned as a dict object.

        Notes
        -----
        This method enables legacy code operation, returning order/trade
        objects as a dictionary.
        """
        return self.__dict__


class Symbol:
    """AutoTrader Symbol object.

    Attributes
    ----------
    instrument : any
        The trade instrument of the position.
    exchange: str
        Exchange for an instrument. This can be NSE, BSE, MCX, NFO
    expiry: any
        Expiry for which symbol token has to be fetched. Takes Values such as "CURRENT, NEAR, FAR" or Date
    product:
        Type of product for instrument. It can be "EQ","FI","FS","OI","OS"
    """

    def __init__(
        self,
        instrument: any = None,
        strategy_parameters: dict = None,
        **kwargs,
    ) -> Symbol:
        self.instrument = instrument
        self.strategy_parameters = strategy_parameters


    def get_order_token_details(self, order_instruments = None):
        if isinstance(self.instrument, list):
            logging.info(f"Generating Symbol for instruments {self.instrument}")
            tokenlist = []
            for inst in self.instrument:
                expiry = options.getExpiryDate(self.strategy_parameters['expiry'],self.strategy_parameters['contract'] )
                api_url = "http://127.0.0.1:8000/tokens/kotak"
                instrument_info = {
                    "instrument": self.strategy_parameters['name'],
                    "exchange": self.strategy_parameters['exchange'],
                    "product": self.strategy_parameters['product'],
                    "expiry": expiry,
                    "strike": inst['strike'],
                    "right": inst['option_type'],
                    "instrument_code": self.strategy_parameters['iciciCode']
                }
                response = requests.post(api_url, json=instrument_info)
                json_response = json.loads(response.content)
                json_response.update(instrument_info)
                tokenlist.append(json_response)
            return tokenlist

    def get_orderbook(self, token):
        if isinstance(self.instrument, list):
            logging.info(f"Getting Orderbook for instruments {self.instrument}")

            expiry = options.getExpiryDate(self.strategy_parameters['expiry'],self.strategy_parameters['contract'] )
            api_url = "http://127.0.0.1:8000/quotes"
            instrument_info = {
                "instrument": self.strategy_parameters['name'],
                "exchange": self.strategy_parameters['exchange'],
                "product": self.strategy_parameters['product'],
                "expiry": expiry,
                "strike": inst['strike'],
                "right": inst['option_type']
            }
            response = requests.post(api_url, json=instrument_info)
            json_response = json.loads(response.content)

        return json_response

    def get_data_token_details(self, token):

        api_url = f"http://127.0.0.1:8000/exchangeTokens/{token}"

        response = requests.get(api_url)
        exchange_token = json.loads(response.content)
        return exchange_token

    def subscribe_websocket(self, token):
        api_url = f"http://127.0.0.1:8000/feed/live/{token}"
        response = requests.get(api_url)
        return response


    def find_trade_instruments(self):
        expiry = options.getExpiryDateIcici(self.strategy_parameters['expiry'], self.strategy_parameters['contract'])
        # Get strike to sell for call
        call_strike = options.getStrikeByPrice(self.strategy_parameters['iciciCode'], self.strategy_parameters['option_price'],
                                               "call", expiry)

        # Get strike to sell for put
        put_strike = options.getStrikeByPrice(self.strategy_parameters['iciciCode'], self.strategy_parameters['option_price'],
                                              "put",
                                              expiry)

        # Set Instruments based on ATM Strike
        if (self.strategy_parameters['option_type'] == "both"):
            self.instrument = [{"strike": call_strike, "option_type": "CE"},
                               {"strike": put_strike, "option_type": "PE"}]


        trade_instrument_details = self.get_order_token_details()

        for instrument in trade_instrument_details:
            self.subscribe_websocket(instrument.get('exchangeToken'))

        return trade_instrument_details
