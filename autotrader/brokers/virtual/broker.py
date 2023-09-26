from __future__ import annotations
import os
import pickle
import numpy as np
import pandas as pd
from decimal import Decimal
from typing import Callable
from datetime import date, datetime
import pytz
import logging
import requests, json
from autotrader_custom_repo.AutoTrader.autotrader import options
from autotrader_custom_repo.AutoTrader.autotrader.autodata import AutoData
from autotrader_custom_repo.AutoTrader.autotrader.utilities import get_data_config
from autotrader_custom_repo.AutoTrader.autotrader.brokers.broker_utils import BrokerUtils
from autotrader_custom_repo.AutoTrader.autotrader.brokers.trading import Order, IsolatedPosition, Position, Trade


class Broker(AbstractBroker):
    """Autotrader Virtual Broker for simulated trading.

    Attributes
    ----------
    verbosity : int
        The verbosity of the broker.
    pending_orders : dict
        A dictionary containing pending orders.
    open_orders : dict
        A dictionary containing open orders yet to be filled.
    filled_orders : dict
        A dictionary containing filled orders.
    cancelled_orders : dict
        A dictionary containing cancelled orders.
    open_trades : dict
        A dictionary containing currently open trades (fills).
    closed_trades : dict
        A dictionary containing closed trades.
    base_currency : str
        The base currency of the account. The default is 'AUD'.
    NAV : float
        The net asset value of the account.
    equity : float
        The account equity balance.
    floating_pnl : float
        The floating PnL.
    margin_available : float
        The margin available on the account.
    leverage : int
        The account leverage.
    spread : float
        The average spread to use when opening and closing trades.
    spread_units : str
        The units of the spread (eg. 'price' or 'percentage'). The default
        is 'price'.
    hedging : bool
        Flag whethere hedging is enabled on the account. The default is False.
    margin_closeout : float
        The fraction of margin available at margin call. The default is 0.
    commission_scheme : str
        The commission scheme being used ('percentage', 'fixed_per_unit'
        or 'flat'). The default is 'percentage'.
    commission : float
        The commission value associated with the commission scheme.
    maker_commission : float
        The commission value associated with liquidity making orders.
    taker_commission : float
        The commission value associated with liquidity taking orders.
    """

    def __init__(self, broker_config: dict = None, utils: BrokerUtils = None) -> None:
        """Initialise virtual broker."""
        if broker_config is not None:
            self._verbosity = broker_config["verbosity"]
        else:
            self._verbosity = 0
        self._utils = utils

        # Orders
        self._pending_orders = {}  # {instrument: {id: Order}}
        self._open_orders = {}  # {instrument: {id: Order}}
        self._filled_orders = {}
        self._cancelled_orders = {}
        self._order_id_instrument = {}  # mapper from order_id to instrument

        # Isolated positions (formerly "trades")
        # self._open_iso_pos = {}
        # self._closed_iso_pos = {}
        self._trade_id_instrument = {}  # mapper from trade_id to instrument

        # Positions
        self._positions = {}
        self._closed_positions = {}

        # Margin call flag
        self._margin_calling = False

        # Fills (executed trades)
        self._fills = []

        # Slippage model
        self._default_slippage_model = self._zero_slippage_model
        self._slippage_models = {}

        # Account
        self._base_currency = "AUD"
        self._NAV = 0  # Net asset value
        self._equity = 0  # Account equity (balance)
        self._floating_pnl = 0
        self._margin_available = 0
        self._open_interest = 0
        self._long_exposure = 0
        self._short_exposure = 0
        self._long_unrealised_pnl = 0
        self._short_unrealised_pnl = 0

        # Evolving metrics
        self._long_realised_pnl = 0
        self._short_realised_pnl = 0

        # Margin
        self._leverage = 1  # The account leverage
        self._spread = 0  # The bid/ask spread
        self._spread_units = "price"  # The units of the spread
        self._hedging = False  # Allow simultaneous trades on opposing sides
        self._margin_closeout = 0.0  # Fraction at margin call

        # Funding rate (for perpetual contracts)
        self._charge_funding = False
        self._funding_rate_history = None
        self._funding_history = []
        self._update_freq = None  # Backtest trading update frequency

        # Commissions
        self._commission_scheme = (
            "percentage"  # Either percentage, fixed_per_unit or flat
        )
        self._commission = 0
        self._maker_commission = 0  # Liquidity 'maker' trade commission
        self._taker_commission = 0  # Liquidity 'taker' trade commission

        # History
        self._latest_time = None

        # Last order and trade counts
        self._last_order_id = 0
        self._last_trade_id = 0
        self._last_fill_id = 0

        # Paper trading mode
        self._paper_trading = False  # Paper trading mode boolean
        self._public_trade_access = False  # Use public trades to update orders
        self.autodata = None  # AutoData instance
        self._state = None  # Last state snapshot
        self._picklefile = None  # Pickle filename

        # CCXT unification
        self.exchange = ""

    def __repr__(self):
        data_feed = self.autodata._feed
        if data_feed == "ccxt":
            data_feed = self.autodata._ccxt_exchange
        return f"AutoTrader Virtual Broker ({data_feed} data feed)"

    def __str__(self):
        return self.__repr__()

    def configure(
        self,
        verbosity: int = None,
        initial_balance: float = None,
        leverage: int = None,
        spread: float = None,
        spread_units: str = None,
        commission: float = None,
        commission_scheme: str = None,
        maker_commission: float = None,
        taker_commission: float = None,
        hedging: bool = None,
        base_currency: str = None,
        paper_mode: bool = None,
        public_trade_access: bool = None,
        margin_closeout: float = None,
        default_slippage_model: Callable = None,
        slippage_models: dict = None,
        charge_funding: bool = None,
        funding_history: pd.DataFrame = None,
        autodata_config: dict = None,
        picklefile: str = None,
        **kwargs,
    ):
        """Configures the broker and account settings.

        Parameters
        ----------
        verbosity : int, optional
            The verbosity of the broker. The default is 0.
        initial_balance : float, optional
            The initial balance of the account, specified in the
            base currency. The default is 0.
        leverage : int, optional
            The leverage available. The default is 1.
        spread : float, optional
            The bid/ask spread to use in backtest (specified in units
            defined by the spread_units argument). The default is 0.
        spread_units : str, optional
            The unit of the spread specified. Options are 'price', meaning
            that the spread is quoted in price units, or 'percentage',
            meaning that the spread is quoted as a percentage of the
            market price. The default is 'price'.
        commission : float, optional
            Trading commission as percentage per trade. The default is 0.
        commission_scheme : str, optional
            The method with which to apply commissions to trades made. The
            options are (1) 'percentage', where the percentage specified by
            the commission argument is applied to the notional trade value,
            (2) 'fixed_per_unit', where the monetary value specified by the
            commission argument is multiplied by the number of units in the
            trade, and (3) 'flat', where a flat monetary value specified by
            the commission argument is charged per trade made, regardless
            of size. The default is 'percentage'.
        maker_commission : float, optional
            The commission to charge on liquidity-making orders. The default
            is None, in which case the nominal commission argument will be
            used.
        taker_commission: float, optional
            The commission to charge on liquidity-taking orders. The default
            is None, in which case the nominal commission argument will be
            used.
        hedging : bool, optional
            Allow hedging in the virtual broker (opening simultaneous
            trades in oposing directions). The default is False.
        base_currency : str, optional
            The base currency of the account. The default is AUD.
        paper_mode : bool, optional
            A boolean flag to indicate if the broker is in paper trade mode.
            The default is False.
        public_trade_access : bool, optional
            A boolean flag to signal if public trades are being used to
            update limit orders. The default is False.
        margin_closeout : float, optional
            The fraction of margin usage at which a margin call will occur.
            The default is 0.
        default_slippage_model : Callable, optional
            The default model to use when calculating the percentage slippage
            on the fill price, for a given order size. The default functon
            returns zero.
        slippage_models : dict, optional
            A dictionary of callable slippage models, keyed by instrument.
        charge_funding : bool, optional
            A boolean flag to charge funding rates. The default is False.
        funding_history : pd.DataFrame, optional
            A DataFrame of funding rate histories for instruments being traded,
            to backtest trading perpetual futures.
            This is a single frame with as many columns as instruments being
            traded. If an instrument is not present, the funding rate will be
            zero.
        picklefile : str, optional
            The filename of the picklefile to load state from. If you do not
            wish to load from state, leave this as None. The default is None.
        """
        self._verbosity = verbosity if verbosity is not None else self._verbosity
        self._leverage = leverage if leverage is not None else self._leverage
        self._commission = commission if commission is not None else self._commission
        self._commission_scheme = (
            commission_scheme
            if commission_scheme is not None
            else self._commission_scheme
        )
        self._spread = spread if spread is not None else self._spread
        self._spread_units = (
            spread_units if spread_units is not None else self._spread_units
        )
        self._base_currency = (
            base_currency if base_currency is not None else self._base_currency
        )
        self._paper_trading = (
            paper_mode if paper_mode is not None else self._paper_trading
        )
        self._public_trade_access = (
            public_trade_access
            if public_trade_access is not None
            else self._public_trade_access
        )
        self._margin_closeout = (
            margin_closeout if margin_closeout is not None else self._margin_closeout
        )
        self._hedging = hedging if hedging is not None else self._hedging
        self._picklefile = picklefile if picklefile is not None else self._picklefile

        # Assign commissions for making and taking liquidity
        self._maker_commission = (
            maker_commission if maker_commission is not None else self._commission
        )
        self._taker_commission = (
            taker_commission if taker_commission is not None else self._commission
        )

        # Configure slippage models
        self._default_slippage_model = (
            default_slippage_model
            if default_slippage_model is not None
            else self._default_slippage_model
        )
        self._slippage_models = (
            slippage_models if slippage_models is not None else self._slippage_models
        )

        # Configure funding rate mechanics
        self._charge_funding = (
            charge_funding if charge_funding is not None else self._charge_funding
        )
        self._funding_rate_history = (
            funding_history
            if funding_history is not None
            else self._funding_rate_history
        )

        if autodata_config is not None:
            # Instantiate AutoData from config
            data_config = get_data_config(
                **autodata_config,
            )
            self.autodata = AutoData(data_config, **autodata_config)

        else:
            # Create local data instance
            self.autodata = AutoData()

        # Initialise balance
        if initial_balance is not None:
            self._make_deposit(initial_balance)

        # Check for pickled state
        if self._paper_trading and self._picklefile is not None:
            # Load state
            if os.path.exists(picklefile):
                self._load_state()

    def get_NAV(self) -> float:
        """Returns Net Asset Value of account."""
        return self._NAV

    def get_balance(self) -> float:
        """Returns balance of account."""
        return self._equity

    def place_order(self, order: Order, **kwargs) -> None:
        """Place order with broker."""
        # Call order to set order time
        datetime_stamp = (
            kwargs["order_time"]
            if "order_time" in kwargs
            else datetime.now(timezone.utc)
        )
        order(order_time=datetime_stamp)

        # Define reference price
        if order.order_type == "limit" or order.order_type == "stop-limit":
            # Use limit price
            ref_price = order.order_limit_price

        else:
            # Use order price
            ref_price = order.order_price

        # Convert stop distance to price
        if order.stop_loss is None and order.stop_distance:
            order.stop_loss = (
                ref_price - order.direction * order.stop_distance * order.pip_value
            )

        # Verify SL price
        invalid_order = False
        if order.stop_loss and order.direction * (ref_price - order.stop_loss) < 0:
            direction = "long" if order.direction > 0 else "short"
            SL_placement = "below" if order.direction > 0 else "above"
            reason = (
                "Invalid stop loss request: stop loss must be "
                + f"{SL_placement} the order price for a {direction}"
                + " trade order.\n"
                + f"Order Price: {ref_price}\nStop Loss: {order.stop_loss}"
            )
            invalid_order = True

        # Verify TP price
        if (
            order.take_profit is not None
            and order.direction * (ref_price - order.take_profit) > 0
        ):
            direction = "long" if order.direction > 0 else "short"
            TP_placement = "above" if order.direction > 0 else "below"
            reason = (
                "Invalid take profit request: take profit must be "
                + f"{TP_placement} the order price for a {direction}"
                + " trade order.\n"
                + f"Order Price: {ref_price}\nTake Profit: {order.take_profit}"
            )
            invalid_order = True

        # Verify order size
        if order.order_type in ["market", "limit", "stop-limit"] and order.size == 0:
            # Invalid order size
            reason = "Invalid order size (must be non-zero)."
            invalid_order = True

        # Check limit order does not cross book
        try:
            if order.order_type in ["limit"]:
                if self._paper_trading:
                    # Get live midprice
                    orderbook = self.get_orderbook(order.instrument)
                    ref_price = (
                        float(orderbook["bids"][0]["price"])
                        + float(orderbook["asks"][0]["price"])
                    ) / 2
                else:
                    # Use order / stop price
                    ref_price = (
                        order.order_stop_price
                        if order.order_stop_price is not None
                        else order.order_price
                    )
                invalid_order = (
                    order.direction * (ref_price - order.order_limit_price) < 0
                )
                reason = (
                    f"Invalid limit price for {order.__repr__()} "
                    + f"(reference price: {ref_price}, "
                    + f"limit price: {order.order_limit_price})"
                )
        except:
            # Exception, continue
            pass

        # Assign order ID
        order.id = self._get_new_order_id()
        self._order_id_instrument[order.id] = order.instrument

        # Add order to pending_orders dict
        order.status = "pending"
        try:
            self._pending_orders[order.instrument.get('token')][order.id] = order

        except KeyError:
            # Instrument hasn't been in pending orders yet
            self._pending_orders[order.instrument.get('token')] = {order.id: order}

        # Submit order
        if invalid_order:
            # Invalid order, cancel it
            self.cancel_order(order.id, reason, "_pending_orders", datetime_stamp)

        else:
            # Move order to open_orders or leave in pending
            immediate_orders = []
            if order.order_type in immediate_orders or self._paper_trading:
                # Move to open orders
                self._move_order(
                    order,
                    from_dict="_pending_orders",
                    to_dict="_open_orders",
                    new_status="open",
                )

            # Print
            if self._verbosity > 0:
                print(
                    f"{datetime_stamp}: Order {order.id} received: {order.__repr__()}"
                )

    def get_orders(self, instrument: str = None, order_status: str = "open") -> dict:
        """Returns orders of status order_status."""
        all_orders = getattr(self, f"_{order_status}_orders")
        if instrument:
            # Return orders for instrument specified
            try:
                orders = all_orders[instrument]
            except KeyError:
                # There are currently no orders for this instrument
                orders = {}
        else:
            # Return all orders
            orders = {}
            for instr, instr_orders in all_orders.items():
                orders.update(instr_orders)
        return orders.copy()

    def cancel_order(
        self,
        order_id: int,
        reason: str = None,
        from_dict: str = "_open_orders",
        timestamp: datetime = None,
        **kwargs,
    ) -> None:
        """Cancels the order.

        Parameters
        ----------
        order_id : int
            The ID of the order to be cancelled.
        reason : str, optional
            The reason why the order is being cancelled. The default
            is None.
        from_dict: str, optional
            The dictionary currently holding the order. The default is
            'open_orders'.
        timestamp: datetime, optional
            The datetime stamp of the order cancellation. The default is
            None.
        """

        instrument = self._order_id_instrument[order_id]
        from_dict = getattr(self, from_dict)[instrument]
        reason = reason if reason is not None else "User cancelled."

        if instrument not in self._cancelled_orders:
            # Initialise instrument in cancelled_orders
            self._cancelled_orders[instrument] = {}
        self._cancelled_orders[instrument][order_id] = from_dict.pop(order_id)
        self._cancelled_orders[instrument][order_id].reason = reason
        self._cancelled_orders[instrument][order_id].status = "cancelled"

        if self._verbosity > 0 and reason:
            # Print cancel reason to console
            print(f"{timestamp}: Order {order_id} cancelled: {reason}")

    def get_trades(self, instrument: str = None, **kwargs) -> dict:
        """Returns fills for the specified instrument.

        Parameters
        ----------
        instrument : str, optional
            The instrument to fetch trades under. The default is None.
        """

        # Get all instrument fills
        all_trades = self._fills

        if instrument:
            # Specific instrument(s) requested
            trades = [fill for fill in all_trades if fill.instrument == "EUR"]

        else:
            # Return all trades
            trades = all_trades

        # Convert to a dict with keys by id
        trades_dict = {fill.id: fill for fill in all_trades}

        # Return a copy to prevent unintended manipulation
        return trades_dict.copy()


    def get_isolated_positions(
        self, instrument: str = None, status: str = "open", **kwargs
    ):
        """Returns isolated positions for the specified instrument.
        This method has been implemented for the users of Oanda, which treats
        trades as isolated positions.

        Parameters
        ----------
        instrument : str, optional
            The instrument to fetch trades under. The default is None.
        status : str, optional
            The status of the isolated positions to fetch ('open' or 'closed'). The
            default is 'open'.
        """
        # Get all instrument isolated positions
        all_iso_pos = getattr(self, f"_{status}_iso_pos")

        if instrument:
            # Specific instrument(s) requested
            try:
                pos = all_iso_pos[instrument]
            except KeyError or TypeError:
                # No isolated positions for this instrument
                pos = {}
        else:
            # Return all currently open isolated positions
            pos = {}
            for instr, instr_trades in all_iso_pos.items():
                pos.update(instr_trades)

        # Return a copy to prevent unintended manipulation
        return pos.copy()

    def get_trade_details(self, trade_ID: int) -> IsolatedPosition:
        """Returns the trade specified by trade_ID."""
        raise DeprecationWarning(
            "This method is deprecated, and will "
            + "be removed in a future release. Please use the "
            + "get_trades method instead."
        )
        instrument = self._trade_id_instrument[trade_ID]
        return self._open_iso_pos[instrument][trade_ID]

    def get_positions(self, instrument: str = None,  **kwargs) -> dict:
        """Returns the positions held by the account, sorted by
        instrument.

        Parameters
        ----------
        instrument : str, optional
            The trading instrument name (symbol). If 'None' is provided,
            all positions will be returned. The default is None.

        Returns
        -------
        open_positions : dict
            A dictionary containing details of the open positions.

        Notes
        ------
        net_position: refers to the number of units held in the position.

        """

        if instrument:
            # Instrument provided
            if instrument in self._positions:
                return {instrument: self._positions[instrument]}
            else:
                return {}
        else:
            return self._positions.copy()

        if isinstance(instrument, list):
            # instrument provided
            instruments = instrument
        elif instrument:
            instruments = [instrument]
        else:
            # No specific instrument requested, use all
            instruments = list(self._open_iso_pos.keys())

        open_positions = {}
        for instrument in instruments:
            # First get open isolated positions
            open_iso_positions = self.get_isolated_positions(instrument.get('token'))
            if len(open_iso_positions) > 0:
                long_units = 0
                long_PL = 0
                long_margin = 0
                short_units = 0
                short_PL = 0
                short_margin = 0
                total_margin = 0
                pnl = 0
                last_price = None
                trade_IDs = []

                for trade_id, trade in open_iso_positions.items():
                    trade_IDs.append(trade.id)
                    last_price = (
                        trade.last_price if trade.last_price is not None else last_price
                    )
                    total_margin += trade.margin_required
                    pnl += trade.unrealised_PL
                    if trade.direction > 0:
                        # Long trade
                        long_units += trade.size
                        long_PL += trade.unrealised_PL
                        long_margin += trade.margin_required
                    else:
                        # Short trade
                        short_units += trade.size
                        short_PL += trade.unrealised_PL
                        short_margin += trade.margin_required

                # Construct instrument position dict
                net_position = long_units - short_units
                try:
                    net_exposure = net_position * last_price
                except TypeError:
                    # Last price has not been updated yet
                    net_exposure = None
                instrument_position = {
                    "long_units": long_units,
                    "long_PL": long_PL,
                    "long_margin": long_margin,
                    "short_units": short_units,
                    "short_PL": short_PL,
                    "short_margin": short_margin,
                    "total_margin": total_margin,
                    "trade_IDs": trade_IDs,
                    "instrument": instrument,
                    "net_position": net_position,
                    "net_exposure": net_exposure,
                    "last_price": last_price,
                    "pnl": pnl,
                }

                # Create Position instance
                instrument_position = Position(**instrument_position)

                # Append Position to open_positions dict
                open_positions[instrument.get('token')] = instrument_position

        return open_positions.copy()


    def get_margin_available(self) -> float:
        """Returns the margin available on the account."""
        return self._margin_available

    def get_orderbook(self, instrument: str, midprice: float = None) -> OrderBook:
        """Returns the orderbook."""
        # Get public orderbook
        if self._paper_trading:
            # Papertrading, try get realtime orderbook
            try:
                orderbook = self.autodata.L2(
                    instrument, spread_units=self._spread_units, spread=self._spread
                )
            except Exception as e:
                # Fetching orderbook failed, revert to local orderbook
                print("Exception:", e)
                print("  Instrument:", instrument)
                orderbook = self.autodata._local_orderbook(
                    instrument=instrument,
                    spread_units=self._spread_units,
                    spread=self._spread,
                    midprice=midprice,
                )

        else:
            # Backtesting, use local pseudo-orderbook
            orderbook = self.autodata._local_orderbook(
                instrument=instrument,
                spread_units=self._spread_units,
                spread=self._spread,
                midprice=midprice,
            )

        # TODO - Add local orders to the book?

        return orderbook

    def _update_positions(
        self,
        instrument: str,
        candle: pd.Series = None,
        L1: dict = None,
        trade: dict = None,
        symbol = None
    ) -> None:
        """Updates orders and open positions based on the latest data.

        Parameters
        ----------
        instrument : str
            The name of the instrument being updated.
        candle : pd.Series
            An OHLC candle used to update orders and trades.
        L1 : dict, optional
            A dictionary a containing level 1 price snapshot to update
            the positions with. This dictionary must have the keys
            'bid', 'ask', 'bid_size' and 'ask_size'.
        trade : dict, optional
            A public trade, used to update virtual limit orders.
        """

        def stop_trigger_condition(order_stop_price, order_direction):
            """Returns True if the order stop price has been triggered
            else False."""
            if L1 is not None:
                # Use L1 data to trigger
                reference_price = L1["bid"] if order_direction > 0 else L1["ask"]
                triggered = order_direction * (reference_price - order_stop_price) > 0

            else:
                # Use OHLC data to trigger
                triggered = candle.Low < order_stop_price < candle.High
            return triggered

        def get_last_price(trade_direction):
            """Returns the last reference price for a trade. If the
            trade is long, this will refer to the bid price. If short,
            this refers to the ask price."""
            if L1 is not None:
                last_price = L1["bid"] if trade_direction > 0 else L1["ask"]
            else:
                last_price = candle.Close
            return last_price

        def get_market_ref_price(order_direction):
            """Returns the reference price for a market order."""
            if L1 is not None:
                reference_price = L1["bid"] if order_direction > 0 else L1["ask"]
            else:
                reference_price = candle.Open
            return reference_price

        def limit_trigger_condition(order_direction, order_limit_price):
            """Returns True if the order limit price has been triggered
            else False."""
            if L1 is not None:
                # Use L1 data to trigger (based on midprice)
                ref_price = (L1["bid"] + L1["ask"]) / 2
            else:
                # Use OHLC data to trigger
                ref_price = candle.Low if order_direction > 0 else candle.High
            triggered = order_direction * (ref_price - float(order_limit_price)) <= 0
            return triggered

        def process_orders_in_dict(orders):
            for order_id, order in orders.items():
                # Check if order has been cancelled (OCO)
                if order.id not in self._open_orders[order.instrument]:
                    continue

                # Order is still open, process it
                if order.order_type == "market":
                    # Market order type - proceed to fill
                    reference_price = get_market_ref_price(order.direction)
                    if order.order_price is None:
                        order.order_price = reference_price
                    self._process_order(
                        order=order,
                        fill_time=latest_time,
                        reference_price=reference_price,
                    )

                elif "stop" in order.order_type:
                    # Check if order_stop_price has been reached yet
                    if stop_trigger_condition(order.order_stop_price, order.direction):
                        # order_stop_price has been reached
                        if order.order_type == "stop-limit":
                            # Change order type to 'limit'
                            order.order_type = "limit"
                        else:
                            # Stop order triggered - proceed to market fill
                            reference_price = order.order_stop_price
                            order.order_price = reference_price
                            order.order_type = "market"
                            self._process_order(
                                order=order,
                                fill_time=latest_time,
                                reference_price=reference_price,
                            )

                # Check limit orders
                if order.order_type == "limit":
                    # Limit order type
                    if not self._public_trade_access:
                        # Update limit orders based on price feed
                        triggered = limit_trigger_condition(
                            order.direction, order.order_limit_price
                        )
                        if triggered:
                            # Limit price triggered, proceed
                            self._process_order(
                                order=order,
                                fill_time=latest_time,
                                reference_price=order.order_limit_price,
                            )
                    else:
                        # Update limit orders based on trade feed
                        if trade is not None:
                            self._public_trade(instrument, trade)

        # Check for data availability
        if L1 is not None:
            # Using L1 data to update
            latest_time = datetime.now(timezone.utc)

        elif candle is not None:
            # Using OHLC data to update
            latest_time = candle.name
            #This is temporary workaround to work with Developement data
            if isinstance(latest_time, np.int64):
                latest_time = datetime.now()
                utc = pytz.UTC
                latest_time = utc.localize(latest_time)

        else:
            # No new price data
            if trade is None:
                # No trade either, exit
                return
            else:
                # Public trade received, only update limit orders
                self._public_trade(instrument, trade)
                return

        # Open pending orders
        pending_orders = self.get_orders(instrument, "pending")
        for order_id, order in pending_orders.items():
            if latest_time > order.order_time:
                self._move_order(
                    order,
                    from_dict="_pending_orders",
                    to_dict="_open_orders",
                    new_status="open",
                )

        # Update open orders for current instrument
        open_orders = self.get_orders(instrument)
        process_orders_in_dict(open_orders)

        # Check for any SL/TP orders spawned
        currently_open_orders = self.get_orders(instrument=instrument)
        newly_opened_orders = dict(
            set(currently_open_orders.items()) - set(open_orders.items())
        )
        process_orders_in_dict(newly_opened_orders)

        # Update position
        position = self.get_positions(instrument=instrument)
        if position:
            position = position[instrument]
            # Position held, update it
            position.last_price = get_last_price(np.sign(position.net_position))
            position.last_time = latest_time
            position.notional = position.last_price * abs(position.net_position)
            position.pnl = (
                position.net_position
                * (position.last_price - position.avg_price)
                * position.HCF
            )

            # Charge funding fees
            # if self._charge_funding:
            #     # TODO - move to unified backtest/livetrade method
            #     last_settlement = self._funding_rate_history.index.get_indexer([pd.Timestamp(latest_time).floor(self._update_freq)], method="nearest")[0]
            #     last_rate = self._funding_rate_history.iloc[last_settlement][instrument]

            #     # Set to zero to prevent re-charging
            #     self._funding_rate_history.iloc[last_settlement][instrument] = 0

        # Update floating pnl and margin available
        self._update_margin(
            instrument=instrument,
            latest_time=latest_time,
        )

        # Update open position value
        self._NAV = self._equity + self._floating_pnl

        # Update account history
        self._latest_time = latest_time

        # Save state
        if self._paper_trading and self._picklefile is not None:
            self._save_state()

    def _update_all(self):
        """Convenience method to update all open positions when paper trading."""
        # Update orders
        to_pop = []
        for instrument in self._open_orders:
            try:
                # Get price data
                l1 = self.autodata.L1(instrument=instrument)
                self._update_positions(instrument=instrument, L1=l1)
            except Exception as e:
                # Something went wrong
                print(f"Exception when updating orders: {e}\n")

                # Cancel orders for this instrument
                orders = self.get_orders(instrument=instrument)
                for order_id in orders:
                    self.cancel_order(
                        order_id=order_id, reason="exception cancellation"
                    )

                # Also pop this instrument from open orders dict
                to_pop.append(instrument)

        # Pop bad instruments
        for instrument in to_pop:
            self._open_orders.pop(instrument)

        # Update positions
        for instrument in self._positions:
            l1 = self.autodata.L1(instrument=instrument)
            self._update_positions(instrument=instrument, L1=l1)

    def _update_instrument(self, instrument):
        """Convenience method to update a single instrument when paper
        trading.
        """
        # Check for existing orders or position in this instrument
        orders = self.get_orders(instrument=instrument)
        position = self.get_positions(instrument=instrument)
        if len(orders) + len(position) > 0:
            # Order or position exists for this instrument, update it
            l1 = self.autodata.L1(instrument=instrument)
            self._update_positions(instrument=instrument, L1=l1)

    def _process_order(
        self,
        order: Order,
        fill_time: datetime = None,
        reference_price: float = None,
        trade_size: float = None,
    ):
        """Processes an order, either filling or cancelling it.

        Parameters
        -----------
        order : Order
            The order being processed.
        fill_time : datetime, optional
            The time to fill the order.
        reference_price : float, optional
            The order reference price (either market price or order limit price).
        trade_size : float, optional
            The size of a public trade being used to fill orders (papertrade
            mode). The default is None.
        """

        # Check if papertrading
        if self._paper_trading:
            # Use current time as fill time
            tz = fill_time.tzinfo  # inherit timezone of dataset
            if tz is not None:
                fill_time = datetime.now(tz=tz)

        # Check for public trade to fill order
        if trade_size is not None:
            # Fill limit order with trade_size provided
            if trade_size < order.size:
                # Create a new order for the portion to be filled by the trade
                order = Order._partial_fill(order=order, units_filled=trade_size)

                # Assign new order ID
                order.id = self._get_new_order_id()
                self._order_id_instrument[order.id] = order.instrument

                # Move new order to open_orders
                # The original order will remain with reduced size
                self._open_orders[order.instrument][order.id] = order

        order_notional = order.size * reference_price * order.HCF
        margin_required = self._calculate_margin(order_notional)

        if margin_required < self._margin_available or order.reduce_only:
            # Order can be filled
            if order.order_type == "limit":
                # Limit order, use limit price for execution
                fill_price = order.order_limit_price

            elif order.order_type == "market":
                # Market order, trade through the book using reference size
                fill_price = self._trade_through_book(
                    instrument=order.instrument,
                    direction=order.direction,
                    size=order.size,
                    reference_price=reference_price,
                    precision=order.price_precision,
                )
            else:
                # Unrecognised order type
                raise Exception(f"Unrecognised order type: {order.order_type}")

            # Fill order
            self._fill_order(
                last_price=reference_price,
                order=order,
                fill_price=fill_price,
                fill_time=fill_time,
            )

        else:
            # Cancel order
            cancel_reason = (
                "Insufficient margin to fill order "
                + f"(${margin_required} required, ${self._margin_available} "
                + "available)."
            )
            self.cancel_order(
                order_id=order.id, reason=cancel_reason, timestamp=fill_time
            )

    def _modify_position(self, trade: Trade, reduce_only: bool):
        """Modifies the position in a position."""
        if trade.instrument in self._positions:
            # Instrument already has a position
            price_precision = self._positions[trade.instrument].price_precision
            starting_net_position = self._positions[trade.instrument].net_position

            if reduce_only:
                # Make sure the trade can only reduce (not swap) the position
                trade.size = min(
                    trade.size,
                    abs(self._positions[trade.instrument].net_position),
                )

                if trade.direction == np.sign(starting_net_position):
                    # The reduce order is on the wrong side, cancel the fill
                    del self._fills[-1]
                    return

            # Update the position with the trade
            self._positions[trade.instrument]._update_with_fill(
                trade=trade,
            )
            new_net_position = self._positions[trade.instrument].net_position

            # Check if position has been reduced
            if np.sign(new_net_position - starting_net_position) != np.sign(
                starting_net_position
            ):
                # Update account with position pnl
                units_reduced = min(
                    abs(new_net_position - starting_net_position),
                    abs(starting_net_position),
                )
                reduction_direction = np.sign(starting_net_position - new_net_position)
                pnl = (
                    reduction_direction
                    * units_reduced
                    * (
                        trade.fill_price
                        - self._positions[trade.instrument]._prev_avg_price
                    )
                )
                self._adjust_balance(pnl)

                # Update realised PnL metrics
                if np.sign(starting_net_position) > 0:
                    # Long position reduced
                    self._long_realised_pnl += pnl
                else:
                    # Short position reduced
                    self._short_realised_pnl += pnl

            # Check if position is zero
            if round(new_net_position, price_precision) == 0:
                # Move to closed positions (and add exit time)
                popped_position = self._positions.pop(trade.instrument)
                popped_position.exit_time = trade.fill_time
                popped_position.exit_price = trade.fill_price
                if trade.instrument in self._closed_positions:
                    # Append
                    self._closed_positions[trade.instrument].append(popped_position)
                else:
                    # Create new entry
                    self._closed_positions[trade.instrument] = [popped_position]

            elif np.sign(self._positions[trade.instrument].net_position) != np.sign(
                starting_net_position
            ):
                # Position has swapped sides
                pass

        else:
            # Create new position
            self._positions[trade.instrument] = Position._from_fill(
                trade=trade,
            )

    def _fill_order(
        self,
        last_price: float,
        fill_price: float,
        fill_time: datetime,
        order: Order = None,
        liquidation_order: bool = False,
    ) -> None:
        """Marks an order as filled and records the trade as a Fill.

        Parameters
        ----------
        fill_price : float
            The fill price.
        fill_time : datetime
            The time at which the order is filled.
        order : Order, optional
            The order to fill. The default is None, in which case the arguments
            below must be specified.
        liquidation_order : bool, optional
            A flag whether this is a liquidation order from the broker.
        """
        if not liquidation_order:
            # Filling an order changes its status to 'filled'
            self._move_order(
                order=order,
                from_dict="_open_orders",
                to_dict="_filled_orders",
                new_status="filled",
            )

        # Infer attributes from provided order
        instrument = order.instrument
        order_price = order.order_price
        order_time = order.order_time
        order_size = order.size
        order_type = order.order_type
        direction = order.direction
        order_id = order.id
        HCF = order.HCF
        fill_price = round(fill_price, order.price_precision)

        # Check for SL
        if order.stop_loss is not None:
            # Create SL order
            sl_order = Order(
                instrument=order.instrument,
                direction=-order.direction,
                size=abs(order.size),
                order_type="stop",
                order_stop_price=round(order.stop_loss, order.price_precision),
                reduce_only=True,
                id=self._get_new_order_id(),
                parent_order=order.id,
                status="open",
                order_time=fill_time,
                order_price=fill_price,
            )
            # Add to open orders
            try:
                self._open_orders[sl_order.instrument][sl_order.id] = sl_order
            except KeyError:
                self._open_orders[sl_order.instrument] = {sl_order.id: sl_order}

            # Add to map
            self._order_id_instrument[sl_order.id] = sl_order.instrument

        # Check for TP
        if order.take_profit is not None:
            # Create TP order
            tp_order = Order(
                instrument=order.instrument,
                direction=-order.direction,
                size=abs(order.size),
                order_type="limit",
                order_limit_price=round(order.take_profit, order.price_precision),
                reduce_only=True,
                id=self._get_new_order_id(),
                parent_order=order.id,
                status="open",
                price_precision=order.price_precision,
                size_precision=order.size_precision,
                order_time=fill_time,
                order_price=fill_price,
            )
            # Add to open orders
            try:
                self._open_orders[tp_order.instrument][tp_order.id] = tp_order
            except KeyError:
                self._open_orders[tp_order.instrument] = {tp_order.id: tp_order}

            # Add to map
            self._order_id_instrument[tp_order.id] = tp_order.instrument

        # Link SL to TP OCO style
        if order.stop_loss is not None and order.take_profit is not None:
            tp_order.OCO.append(sl_order.id)
            sl_order.OCO.append(tp_order.id)

        # Charge trading fees
        commission = self._calculate_commissions(
            price=fill_price, units=order_size, HCF=HCF, order_type=order_type
        )
        self._adjust_balance(-commission, latest_time=fill_time)

        # Create Trade and append to fills
        # Note that this object may be modified by _modify_position
        trade = Trade(
            instrument=instrument,
            order_price=order_price,
            order_time=order_time,
            size=order_size,
            last_price=last_price,
            fill_time=fill_time,
            fill_price=fill_price,
            fill_direction=direction,
            fee=commission,
            id=self._get_new_fill_id(),
            order_id=order_id,
            order_type=order_type,
            _price_precision=order.price_precision,
            _size_precision=order.size_precision,
        )
        self._fills.append(trade)

        # Check OCO orders
        for order_id in order.OCO:
            self.cancel_order(
                order_id=order_id,
                reason="Linked order cancellation.",
                timestamp=fill_time,
            )

        # Print fill to console
        if self._verbosity > 0:
            id_str = order_id if order_id is not None else ""
            side = "(Buy)" if direction > 0 else "(Sell)"
            fill_str = (
                f"{fill_time}: Order {id_str} filled: {order_size} "
                + f"units of {instrument} @ {fill_price} {side}"
            )
            print(fill_str)

        # Update position with fill
        self._modify_position(trade=trade, reduce_only=order.reduce_only)

    def _move_order(
        self,
        order: Order,
        from_dict: str = "_open_orders",
        to_dict: str = "_filled_orders",
        new_status: str = "filled",
    ) -> None:
        """Moves an order from the from_dict to the to_dict."""
        order.status = new_status
        from_dict = getattr(self, from_dict)[order.instrument.get('token')]
        to_dict = getattr(self, to_dict)
        popped_item = from_dict.pop(order.id)
        try:
            to_dict[order.instrument.get('token')][order.id] = popped_item
        except KeyError:
            to_dict[order.instrument.get('token')] = {order.id: popped_item}

    def _move_isolated_position(
        self,
        trade: IsolatedPosition,
        from_dict: str = None,
        to_dict: str = "_open_iso_pos",
        new_status="open",
    ) -> None:
        """Moves an isolated position from the from_dict to the to_dict."""
        trade.status = new_status
        to_dict = getattr(self, to_dict)
        if from_dict is not None:
            # From dict exists, pop trade from it
            from_dict = getattr(self, from_dict)[trade.instrument.get('token')]
            popped_item = from_dict.pop(trade.id)

        else:
            # Use trade directly
            popped_item = trade

        # Make the move
        try:
            to_dict[trade.instrument.get('token')][trade.id] = popped_item
        except KeyError:
            to_dict[trade.instrument.get('token')] = {trade.id: popped_item}

    def _reduce_position(
        self,
        order: Order = None,
        exit_price: float = None,
        exit_time: datetime = None,
    ) -> None:
        """Reduces the position of the specified instrument using an order. If the
        order.size exceeds the net position, the entire existing position will be
        closed out.

        Parameters
        -----------

        Returns
        -------
        avg_exit_price : float
            The average execution price used when reducing the position.
        """
        # Assign reference price: use limit price for limit order, else market price
        reference_price = (
            order.order_limit_price
            if order.order_limit_price is not None
            else exit_price
        )

        # Get current position in instrument
        position = self.get_positions(order.instrument)

        # Modify existing trades until there are no more units to reduce
        units_to_reduce = min(
            abs(order.size), abs(position[order.instrument].net_position)
        )
        executed_prices = []
        executed_sizes = []
        while round(units_to_reduce, order.size_precision) > 0:
            # There are units to be reduced
            open_trades = self.get_isolated_positions(order.instrument)
            # TODO - issue when a margin call happens during this process,
            # and the open_trades dict is no longer truthful. Need to review
            # margin call process, as reducing should not increase margin
            # requirements
            for trade_id, trade in open_trades.items():
                if trade.direction != order.direction:
                    # Reduce this trade
                    if units_to_reduce >= trade.size:
                        # Entire trade must be closed
                        exit_price = self._close_isolated_position(
                            trade=trade,
                            exit_price=reference_price,
                            exit_time=exit_time,
                            order_type=order.order_type,
                        )

                        # Update units_to_reduce
                        units_to_reduce -= abs(trade.size)
                        executed_prices.append(exit_price)
                        executed_sizes.append(trade.size)

                    elif units_to_reduce > 0:
                        # Partially close trade (0 < units_to_reduce < trade.size)
                        exit_price = self._reduce_isolated_position(
                            trade=trade,
                            units=units_to_reduce,
                            exit_price=reference_price,
                            exit_time=exit_time,
                            order_type=order.order_type,
                        )

                        # Update units_to_reduce
                        executed_prices.append(exit_price)
                        executed_sizes.append(units_to_reduce)
                        units_to_reduce = 0

        avg_exit_price = sum(
            [
                executed_sizes[i] * executed_prices[i]
                for i in range(len(executed_prices))
            ]
        ) / sum(executed_sizes)
        return avg_exit_price

    def _reduce_isolated_position(
        self,
        trade: IsolatedPosition,
        units: float,
        exit_price: float = None,
        exit_time: datetime = None,
        order_type: str = "market",
    ) -> None:
        """Splits an isolated position into two, to reduce it by a fractional
        amount. The original ID remains, but the size is reduced. The portion
        that gets closed is assigned a new ID.

        Returns
        -------
        exit_price : float
            The execution price used to reduce the isolated position.
        """
        # Create new trade for the amount to be reduced
        partial_trade = IsolatedPosition._split(trade, units)
        partial_trade_id = self._get_new_trade_id()
        partial_trade.id = partial_trade_id

        # Add partial trade to open trades, then immediately close it
        self._open_iso_pos[trade.instrument][partial_trade_id] = partial_trade
        exit_price = self._close_isolated_position(
            trade=partial_trade,
            exit_price=exit_price,
            exit_time=exit_time,
            order_type=order_type,
        )

        # Keep track of partial trade id instrument for reference
        self._trade_id_instrument[partial_trade_id] = trade.instrument

        return exit_price

    def _close_isolated_position(
        self,
        trade: IsolatedPosition,
        exit_price: float = None,
        exit_time: datetime = None,
        order_type: str = "market",
    ) -> None:
        """Closes an isolated position by ID.

        Parameters
        ----------
        trade_id : int, optional
            The trade id. The default is None.
        exit_price : float, optional
            The trade exit price. If none is provided, the market price
            will be used. The default is None.
        exit_time : datetime, optional
            The trade exit time. The default is None.
        order_type : str, optional
            The order type used to calculate commissions. The default
            is 'market'.

        Returns
        -------
        exit_price : float
            The price executed for the closeout of the isolated position.
        """
        fill_price = trade.fill_price
        size = trade.size
        direction = trade.direction

        reference_price = exit_price if exit_price is not None else trade.last_price
        if order_type == "limit":
            # Limit order, use exit price provided
            exit_price = reference_price

        else:
            # Trade through book, exit price provided as reference for midprice
            exit_price = self._trade_through_book(
                instrument=trade.instrument,
                direction=-direction,
                size=size,
                reference_price=reference_price,
                precision=trade.price_precision,
            )

        # Update portfolio with profit/loss
        pnl = direction * size * (float(exit_price) - float(fill_price)) * trade.HCF

        # Update trade closure attributes
        trade.profit = pnl
        trade.balance = self._equity
        trade.exit_price = exit_price
        # trade.fees = commission
        trade.exit_time = exit_time if exit_time is not None else trade.last_time

        # Add trade to closed positions
        self._move_isolated_position(
            trade,
            from_dict="_open_iso_pos",
            to_dict="_closed_iso_pos",
            new_status="closed",
        )

        # Update account with position pnl
        self._adjust_balance(pnl)

        return exit_price

    def _trade_through_book(
        self,
        instrument: str,
        direction: int,
        size: float,
        reference_price: float = None,
        precision: int = None,
    ) -> float:
        """Returns an average fill price by filling an order through
        the orderbook.

        Parameters
        -----------
        instrument : str
            The instrument to fetch the orderbook for.
        direction : int
            The direction of the trade (1 for long, -1 for short). Used
            to specify either bid or ask prices.
        size : float
            The size of the trade.
        reference_price : float, optional
            The reference price to use if artificially creating an
            orderbook.
        precision : dict, optional
            The precision to use for rounding prices. The default
            is None.
        """
        # Get order book
        book = self.get_orderbook(instrument, reference_price)
        # # Work through the order book
        # units_to_fill = size
        # side = "bid" if direction < 0 else "ask"
        # fill_prices = []
        # fill_sizes = []
        # level_no = 0
        # while units_to_fill > 0:
        #
        #     # Consume liquidity
        #     level = getattr(book, side).iloc[level_no]
        #     units_consumed = min(units_to_fill, float(level["size"]))
        #     fill_prices.append(float(level["price"]))
        #     fill_sizes.append(units_consumed)
        #
        #     # Iterate
        #     level_no += 1
        #     units_to_fill -= units_consumed
        #
        # avg_fill_price = sum(
        #     [fill_sizes[i] * fill_prices[i] for i in range(len(fill_prices))]
        # ) / sum(fill_sizes)
        avg_fill_price = book['midprice']

        # Apply slippage function
        if not self._paper_trading:
            # Currently backtesting - apply slippage function to fill price
            slippage_model = self._slippage_models.setdefault(
                instrument,
                self._default_slippage_model,
            )
            slippage_pc = slippage_model(size)
            avg_fill_price *= 1 + direction * slippage_pc

        if precision is not None:
            # Round price to precision
            avg_fill_price = round(avg_fill_price, precision)

        return avg_fill_price

    def _calculate_commissions(
        self,
        price: float,
        units: float = None,
        HCF: float = 1,
        order_type: str = "market",
    ) -> float:
        """Calculates trade commissions."""
        # Get appropriate commission value
        commission_val = (
            self._taker_commission if order_type == "market" else self._maker_commission
        )

        if self._commission_scheme == "percentage":
            # Commission charged as percentage of trade value
            trade_value = abs(units) * float(price) * HCF
            commission = (commission_val / 100) * trade_value

        elif self._commission_scheme == "fixed_per_unit":
            # Fixed commission per unit traded
            commission = commission_val * units

        elif self._commission_scheme == "flat":
            # Flat commission value per trade
            commission = commission_val

        return commission

    def _adjust_balance(self, amount: float, latest_time: datetime = None) -> None:
        """Adjusts the balance of the account."""
        self._equity += amount
        self._update_margin(latest_time=latest_time)

    def _make_deposit(self, deposit: float) -> None:
        """Adds deposit to account balance and NAV."""
        self._equity += deposit
        self._NAV += deposit
        self._update_margin()

    def _calculate_margin(self, position_value: float) -> float:
        """Calculates margin required to take a position with the
        available leverage of the account.
        """
        margin = position_value / self._leverage
        return margin

    def _update_margin(
        self, instrument: str = None, latest_time: datetime = None
    ) -> None:
        """Updates the margin available in the account."""
        # TODO - only update with instrument
        margin_used = 0
        floating_pnl = 0
        open_interest = 0

        positions = self.get_positions()
        long_exposure = 0
        short_exposure = 0
        long_upnl = 0
        short_upnl = 0
        for instrument, position in positions.items():
            margin_used += self._calculate_margin(position.notional)
            floating_pnl += position.pnl
            open_interest += position.notional

            if position.direction > 0:
                long_upnl += position.pnl
                long_exposure += position.notional
            else:
                short_upnl += position.pnl
                short_exposure += position.notional

        # Update unrealised PnL
        self._long_unrealised_pnl = long_upnl
        self._short_unrealised_pnl = short_upnl
        self._long_exposure = long_exposure
        self._short_exposure = short_exposure
        self._floating_pnl = floating_pnl

        # Update margin available
        self._margin_available = self._NAV - margin_used

        # Update open interest
        self._open_interest = open_interest

        # Check for margin call
        if (
            self._leverage > 1
            and self._margin_available / self._NAV < self._margin_closeout
            and not self._margin_calling
        ):
            # Margin call - close all positions
            self._margin_calling = True
            if self._verbosity > 0:
                print("MARGIN CALL: closing all positions.")
            positions = self.get_positions()
            for instrument, position in positions.items():
                last_price = position.last_price
                self._margin_call(instrument, latest_time, last_price)

            # Reset margin call flag
            self._margin_calling = False

    def _get_new_order_id(self):
        self._last_order_id += 1
        return self._last_order_id

    def _get_new_trade_id(self):
        self._last_trade_id += 1
        return self._last_trade_id

    def _get_new_fill_id(self):
        self._last_fill_id += 1
        return self._last_fill_id

    def _margin_call(self, instrument: str, latest_time: datetime, latest_price: float):
        """Closes the open position of the instrument."""
        # Construct full position closeout market order
        position = self.get_positions(instrument=instrument)[instrument]
        size = abs(position.net_position)
        direction = np.sign(position.net_position)
        ref_time = latest_time
        ref_price = latest_price
        ref_id = self._get_new_order_id()
        closeout_order = Order(
            instrument=instrument,
            order_price=ref_price,
            order_time=ref_time,
            direction=-direction,
            size=size,
            order_type="market",
            id=ref_id,
            HCF=1,
        )

        # Fire the order
        fill_price = self._trade_through_book(
            instrument=instrument,
            direction=-direction,
            size=size,
            reference_price=latest_price,
        )
        self._fill_order(
            last_price=latest_price,
            fill_price=fill_price,
            fill_time=ref_time,
            order=closeout_order,
            liquidation_order=True,
        )

    def _add_orders_to_book(self, instrument, orderbook):
        """Adds local orders to the orderbook."""
        # TODO - implement
        orders = self.get_orders(instrument)
        for order in orders:
            if order.order_type == "limit":
                side = "bids" if order.direction > 0 else "asks"

                # Add to the book
                orderbook[side]

        return orderbook

    @staticmethod
    def _zero_slippage_model(*args, **kwargs):
        "Returns zero slippage."
        return 0

    def _save_state(self):
        """Pickles the current state of the broker."""
        try:
            # Remove old picklefile (if it exists)
            os.remove(self._picklefile)
        except:
            pass

        with open(self._picklefile, "wb") as file:
            pickle.dump(self, file)

    def _load_state(self):
        """Loads the state of the broker from a pickle."""
        verbosity = self._verbosity
        try:
            with open(self._picklefile, "rb") as file:
                state = pickle.load(file)

            # Overwrite present state from pickle
            for key, item in state.__dict__.items():
                self.__dict__[key] = item

            if verbosity > 0:
                print("Virtual broker state loaded from pickle.")
        except:
            print("Failed to load virtual broker state.")

    def _public_trade(self, instrument: str, trade: dict):
        """Uses a public trade to update virtual orders."""
        # TODO - use a Trade object
        trade_direction = trade["direction"]
        trade_price = trade["price"]
        trade_size = trade["size"]
        trade_time = trade["time"]

        trade_units_remaining = trade_size
        open_orders = self.get_orders(instrument).copy()
        for order_id, order in open_orders.items():
            if order.order_type == "limit":
                if order.direction != trade_direction:
                    # Buy trade for sell orders, Sell trade for buy orders
                    order_price = Decimal(str(order.order_limit_price)).quantize(
                        Decimal(str(trade_price))
                    )
                    if trade_price == order_price and trade_units_remaining > 0:
                        # Fill as much as possible
                        trade_units_consumed = min(trade_units_remaining, order.size)
                        self._process_order(
                            order=order,
                            fill_time=trade_time,
                            reference_price=order.order_limit_price,
                            trade_size=trade_units_consumed,
                        )

                        # Update trade_units_remaining
                        trade_units_remaining -= trade_units_consumed
