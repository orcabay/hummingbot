from hummingbot.market.in_flight_order_base cimport InFlightOrderBase

cdef class BitstampInFlightOrder(InFlightOrderBase):
    cdef:
        int _last_transaction_id
