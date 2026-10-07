package com.example.model;

/**
 * A child record that waits in the state store for its parent.
 *
 * @param key          the key of the child in its source topic
 * @param value        the value of the child in its source topic
 * @param timestamp    the record timestamp of the child, kept for the output record
 * @param waitingSince wall-clock time when the child started to wait
 */
public record PendingChild(String key, String value, long timestamp, long waitingSince) {
}
