package edu.hse.concurrency.booking;

import java.time.Duration;

final class Waiter {

    boolean registered;
    private boolean signaled;

    synchronized void signal() {
        signaled = true;
        notifyAll();
    }

    synchronized void await(Duration timeout, long started)
            throws InterruptedException {
        while (!signaled) {
            long left = BookingServiceImpl.remaining(timeout, started);
            if (left == 0) {
                return;
            }
            BookingServiceImpl.timedWait(this, left);
        }
    }
}
