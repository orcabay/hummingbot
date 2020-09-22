from decimal import Decimal
from typing import (
    Any,
    Dict
)

from hummingbot.core.event.events import (
    OrderType,
    TradeType
)
from hummingbot.connector.in_flight_order_base import InFlightOrderBase


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
            client_order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            initial_state  # Open, Finished, Cancelled
        )
        self._last_transaction_id = 0

    @property
    def is_done(self) -> bool:
        return self.last_state in {"Finished", "Canceled"}

    @property
    def is_cancelled(self) -> bool:
        return self.last_state in {"Canceled"}

    @property
    def is_failure(self) -> bool:
        return self.last_state in {"Finished"}

    @property
    def is_open(self) -> bool:
        return self.last_state in {"Open"}

    @property
    def last_transaction_id(self) -> int:
        return self._last_transaction_id

    @last_transaction_id.setter
    def last_transaction_id(self, new_id: int):
        self._last_transaction_id = new_id

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
