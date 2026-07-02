package io.jprequal.core;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinSelector implements ReplicaSelector {
    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public String select(List<String> replicas) {
        int index = counter.getAndIncrement() % replicas.size();

        return replicas.get(index);
    }
}
