package io.jprequal.core;

public record Probe(String replica, long timestamp, int rif, int latencyEstimate) {
}
