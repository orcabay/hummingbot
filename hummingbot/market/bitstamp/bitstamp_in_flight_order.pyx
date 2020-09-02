from decimal import Decimal
from typing import (
    Any,
    Dict,
    Optional
)

from hummingbot.core.event.events import (
    OrderType,
    TradeType
)
from hummingbot.market.bitstamp.bitstamp_market import BitstampMarket
from hummingbot.market.in_flight_order_base import InFlightOrderBase


cdef class BitstampInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: str,
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 initial_state: str = "Open"):
        super().__init__(
            BitstampMarket,
            client_order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            initial_state  # Open, Finished, Cancelled
        )
        self.last_transaction_id = 0

    @property
    def is_done(self) -> bool:
        return self.last_state is "Finished"

    @property
    def is_cancelled(self) -> bool:
        return self.last_state is "Cancelled"

    @property
    def is_failure(self) -> bool:
        return self.last_state is "Finished"

    @property
    def is_open(self) -> bool:
        return self.last_state is "Open"

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> InFlightOrderBase:
        cdef:
            BitstampInFlightOrder retval = BitstampInFlightOrder(
                client_order_id=data["client_order_id"],
                exchange_order_id=data["exchange_order_id"],
                trading_pair=data["trading_pair"],
                order_type=getattr(OrderType, data["order_type"]),
                trade_type=getattr(TradeType, data["trade_type"]),
                price=Decimal(data["price"]),
                amount=Decimal(data["amount"]),
                initial_state=data["last_state"]
            )
        retval.executed_amount_base = Decimal(data["executed_amount_base"])
        retval.executed_amount_quote = Decimal(data["executed_amount_quote"])
        retval.fee_asset = data["fee_asset"]
        retval.fee_paid = Decimal(data["fee_paid"])
        retval.last_state = data["last_state"]
        return retval
