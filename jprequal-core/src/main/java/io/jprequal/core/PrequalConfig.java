package io.jprequal.core;

import java.util.List;

/**
 * Validated Prequal parameters. Rates may be fractional (paper, footnote 7);
 * they are randomly rounded per query to preserve the expectation.
 */
public record PrequalConfig(
        List<String> replicas,
        int maxSize,
        double probingRate,
        double rremove,
        double delta,
        int probeTimeoutMs,
        int maxFailures,
        double qRif,
        long probeStalenessMs,
        long recoveryIntervalMs) {

    public PrequalConfig {
        if (replicas == null || replicas.isEmpty()) {
            throw new IllegalArgumentException("replicas cannot be empty");
        }
        replicas = List.copyOf(replicas);
        if (maxSize <= 0) {
            throw new IllegalArgumentException("max_size must be greater than 0");
        }
        if (maxSize >= replicas.size()) {
            throw new IllegalArgumentException("max_size (" + maxSize + ") must be less than the number of replicas ("
                    + replicas.size() + "); the breuse formula (paper eq. 1) requires m < n");
        }
        if (probingRate <= 0) {
            throw new IllegalArgumentException("probing_rate must be greater than 0");
        }
        if (rremove < 0) {
            throw new IllegalArgumentException("rremove cannot be negative");
        }
        if (probingRate <= rremove) {
            throw new IllegalArgumentException("probing_rate (" + probingRate + ") must exceed rremove ("
                    + rremove + ") or the probe pool will drain");
        }
        if (delta <= 0) {
            throw new IllegalArgumentException("delta must be greater than 0");
        }
        if (probeTimeoutMs <= 0) {
            throw new IllegalArgumentException("probe_timeout_ms must be greater than 0");
        }
        if (maxFailures <= 0) {
            throw new IllegalArgumentException("max_failures must be greater than 0");
        }
        if (qRif < 0.0 || qRif > 1.0) {
            throw new IllegalArgumentException("q_rif must be in [0, 1]");
        }
        if (probeStalenessMs <= 0) {
            throw new IllegalArgumentException("probe_staleness_ms must be greater than 0");
        }
        if (recoveryIntervalMs <= 0) {
            throw new IllegalArgumentException("recovery_interval_ms must be greater than 0");
        }
    }
}
