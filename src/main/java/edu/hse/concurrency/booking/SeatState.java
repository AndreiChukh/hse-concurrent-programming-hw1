package edu.hse.concurrency.booking;

import java.util.HashSet;
import java.util.Set;

final class SeatState {

    Hold hold;
    boolean sold;
    final Set<Waiter> waiters = new HashSet<>();
}
