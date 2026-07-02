package io.jprequal.core;

import java.util.List;

public interface ReplicaSelector {
    String select(List<String> replicas);
}