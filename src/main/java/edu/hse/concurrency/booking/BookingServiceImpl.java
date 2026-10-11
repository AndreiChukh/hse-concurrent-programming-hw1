
package edu.hse.concurrency.booking;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;

public class BookingServiceImpl implements BookingService {

    private final Hall hall;
    private final Duration holdDuration;
    private final SeatState[] states;
    private final Object lifecycle = new Object();
    private final Object schedule = new Object();
    private final Set<Waiter> waiting = new HashSet<>();
    private final PriorityQueue<Hold> expirations = new PriorityQueue<>(
            (a, b) -> a.expiresAt().compareTo(b.expiresAt()));
    private final Thread expirationThread;
    private volatile boolean stopped;
    private long nextId;

    public BookingServiceImpl(Hall hall, Duration holdDuration) {
        this.hall = Objects.requireNonNull(hall, "hall");
        this.holdDuration = Objects.requireNonNull(holdDuration, "holdDuration");
        if (holdDuration.isZero() || holdDuration.isNegative()) {
            throw new IllegalArgumentException("holdDuration must be positive");
        }
        states = new SeatState[hall.capacity()];
        Arrays.setAll(states, i -> new SeatState());
        expirationThread = new Thread(this::expireLoop, "booking-expiration");
        expirationThread.start();
    }

    @Override
    public Optional<Hold> hold(String clientId, Set<Seat> seats) {
        Set<Seat> copy = validateRequest(clientId, seats);
        int[] indices = indices(copy);
        ensureRunning();
        return locked(indices, 0, () -> {
            ensureRunning();
            return availability(indices, Instant.now()) == SeatStatus.FREE
                    ? Optional.of(createHold(clientId, copy, indices)) : Optional.empty();
        });
    }

    @Override
    public Optional<Hold> holdOrWait(String clientId, Set<Seat> seats, Duration timeout)
            throws InterruptedException {
        Set<Seat> copy = validateRequest(clientId, seats);
        int[] indices = indices(copy);
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative");
        }
        long started = System.nanoTime();
        ensureRunning();
        boolean first = true;
        while (true) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            Waiter waiter = new Waiter();
            Hold hold = tryHoldOrRegister(clientId, copy, indices, timeout, started,
                    first && timeout.isZero(), waiter);
            if (!waiter.registered) {
                if (hold == null && Thread.interrupted()) {
                    throw new InterruptedException();
                }
                return Optional.ofNullable(hold);
            }
            try {
                waiter.await(timeout, started);
            } finally {
                unregister(indices, waiter);
            }
            first = false;
        }
    }

    private Hold tryHoldOrRegister(String clientId, Set<Seat> seats, int[] indices,
                                   Duration timeout, long started, boolean immediate, Waiter waiter) {
        return locked(indices, 0, () -> {
            if (Thread.currentThread().isInterrupted() || stopped
                    || (!immediate && remaining(timeout, started) == 0)) {
                return null;
            }
            SeatStatus state = availability(indices, Instant.now());
            if (state == SeatStatus.SOLD) {
                return null;
            }
            synchronized (lifecycle) {
                if (stopped || Thread.currentThread().isInterrupted()
                        || (!immediate && remaining(timeout, started) == 0)) {
                    return null;
                }
                if (state == SeatStatus.FREE) {
                    return createHold(clientId, seats, indices);
                }
                if (remaining(timeout, started) == 0) {
                    return null;
                }
                waiting.add(waiter);
                waiter.registered = true;
            }
            for (int index : indices) {
                states[index].waiters.add(waiter);
            }
            return null;
        });
    }

    private void unregister(int[] indices, Waiter waiter) {
        locked(indices, 0, () -> {
            for (int index : indices) {
                states[index].waiters.remove(waiter);
            }
            synchronized (lifecycle) {
                waiting.remove(waiter);
            }
            return null;
        });
    }

    @Override
    public boolean confirm(Hold hold) {
        return finish(hold, true);
    }

    @Override
    public boolean release(Hold hold) {
        return finish(hold, false);
    }

    @Override
    public SeatStatus status(Seat seat) {
        int index = validateSeat(seat);
        synchronized (states[index]) {
            return stateOf(states[index], Instant.now());
        }
    }

    @Override
    public void shutdown() {
        Set<Waiter> wake;
        synchronized (lifecycle) {
            stopped = true;
            wake = new HashSet<>(waiting);
        }
        for (Waiter waiter : wake) {
            waiter.signal();
        }
        synchronized (schedule) {
            schedule.notifyAll();
        }
        boolean interrupted = false;
        while (true) {
            try {
                expirationThread.join();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        synchronized (schedule) {
            expirations.clear();
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean finish(Hold hold, boolean sell) {
        Objects.requireNonNull(hold, "hold");
        Set<Seat> copy = validateRequest(hold.clientId(), hold.seats());
        int[] indices = indices(copy);
        ensureRunning();
        return locked(indices, 0, () -> {
            synchronized (lifecycle) {
                ensureRunning();
                if (!Instant.now().isBefore(hold.expiresAt())) {
                    return false;
                }
                for (int index : indices) {
                    if (states[index].hold != hold || states[index].sold) {
                        return false;
                    }
                }
                for (int index : indices) {
                    states[index].hold = null;
                    states[index].sold = sell;
                }
            }
            signalWaiters(indices);
            return true;
        });
    }

    private Hold createHold(String clientId, Set<Seat> seats, int[] indices) {
        synchronized (lifecycle) {
            ensureRunning();
            Instant expiresAt;
            try {
                expiresAt = Instant.now().plus(holdDuration);
            } catch (java.time.DateTimeException | ArithmeticException e) {
                expiresAt = Instant.MAX;
            }
            Hold hold = new Hold(++nextId, clientId, seats, expiresAt);
            for (int index : indices) {
                states[index].hold = hold;
            }
            synchronized (schedule) {
                expirations.add(hold);
                schedule.notifyAll();
            }
            return hold;
        }
    }

    private void expireLoop() {
        while (true) {
            Hold expiry;
            synchronized (schedule) {
                while (true) {
                    if (stopped) {
                        return;
                    }
                    expiry = expirations.peek();
                    try {
                        if (expiry == null) {
                            schedule.wait();
                        } else {
                            Duration delay = Duration.between(Instant.now(), expiry.expiresAt());
                            if (delay.isNegative() || delay.isZero()) {
                                expirations.remove();
                                break;
                            }
                            timedWait(schedule, nanos(delay));
                        }
                    } catch (InterruptedException e) {
                        if (stopped) {
                            return;
                        }
                    }
                }
            }
            Hold due = expiry;
            int[] indices = indices(due.seats());
            locked(indices, 0, () -> {
                Set<Waiter> wake = new HashSet<>();
                for (int index : indices) {
                    SeatState state = states[index];
                    if (state.hold == due) {
                        state.hold = null;
                        wake.addAll(state.waiters);
                    }
                }
                for (Waiter waiter : wake) {
                    waiter.signal();
                }
                return null;
            });
        }
    }

    private void signalWaiters(int[] indices) {
        Set<Waiter> wake = new HashSet<>();
        for (int index : indices) {
            wake.addAll(states[index].waiters);
        }
        for (Waiter waiter : wake) {
            waiter.signal();
        }
    }

    private SeatStatus availability(int[] indices, Instant now) {
        SeatStatus result = SeatStatus.FREE;
        for (int index : indices) {
            SeatStatus state = stateOf(states[index], now);
            if (state == SeatStatus.SOLD) {
                return SeatStatus.SOLD;
            }
            if (state == SeatStatus.HELD) {
                result = SeatStatus.HELD;
            }
        }
        return result;
    }

    private SeatStatus stateOf(SeatState state, Instant now) {
        if (state.sold) {
            return SeatStatus.SOLD;
        }
        return state.hold != null && now.isBefore(state.hold.expiresAt())
                ? SeatStatus.HELD : SeatStatus.FREE;
    }

    private Set<Seat> validateRequest(String clientId, Set<Seat> seats) {
        Objects.requireNonNull(clientId, "clientId");
        Objects.requireNonNull(seats, "seats");
        Set<Seat> copy = Set.copyOf(seats);
        if (clientId.isBlank() || copy.isEmpty() || copy.size() > MAX_SEATS_PER_HOLD) {
            throw new IllegalArgumentException("Invalid clientId or number of seats");
        }
        return copy;
    }

    private int validateSeat(Seat seat) {
        Objects.requireNonNull(seat, "seat");
        if (!hall.contains(seat)) {
            throw new IllegalArgumentException("Seat is outside the hall");
        }
        return (seat.row() - 1) * hall.seatsPerRow() + seat.number() - 1;
    }

    private int[] indices(Set<Seat> seats) {
        int[] result = new int[seats.size()];
        int offset = 0;
        for (Seat seat : seats) {
            result[offset++] = validateSeat(seat);
        }
        Arrays.sort(result);
        return result;
    }

    private <T> T locked(int[] indices, int offset, Operation<T> operation) {
        if (offset == indices.length) {
            return operation.run();
        }
        synchronized (states[indices[offset]]) {
            return locked(indices, offset + 1, operation);
        }
    }

    private void ensureRunning() {
        if (stopped) {
            throw new IllegalStateException("Service is stopped");
        }
    }

    private static long nanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    static long remaining(Duration timeout, long started) {
        Duration left = timeout.minusNanos(System.nanoTime() - started);
        return left.isNegative() ? 0 : nanos(left);
    }

    static void timedWait(Object monitor, long nanos) throws InterruptedException {
        monitor.wait(nanos / 1_000_000, (int) (nanos % 1_000_000));
    }

    private interface Operation<T> {
        T run();
    }
}
