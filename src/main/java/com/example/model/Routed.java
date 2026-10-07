package com.example.model;

/**
 * Output of the pre-join processor: a value and the topic it goes to.
 */
public record Routed(Route route, String value) {

    public enum Route {
        /** The parent exists. The child connector can write the child. */
        READY,
        /** The child cannot be released: it waited too long, or it has no parent ID. */
        ERROR,
        /** The parent success record has no parent ID: probably a wrong JSON pointer or an unexpected record format. */
        PARENT_ERROR
    }

    public static Routed ready(String value) {
        return new Routed(Route.READY, value);
    }

    public static Routed error(String value) {
        return new Routed(Route.ERROR, value);
    }

    public static Routed parentError(String value) {
        return new Routed(Route.PARENT_ERROR, value);
    }
}
