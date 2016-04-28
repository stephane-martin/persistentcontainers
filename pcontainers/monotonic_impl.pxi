# -*- coding: utf-8 -*-

cpdef monotonic():
    return steady_now().time_since_epoch().count() / 1000000.0  # in milliseconds

