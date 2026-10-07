package com.example.model;

/**
 * Envelope for the merged parent and child streams.
 * Both sides are re-keyed by the parent ID and go through one repartition topic,
 * so a parent and all its children are always processed by the same task.
 *
 * @param side        where the record came from
 * @param originalKey the key of the record in its source topic
 * @param value       the unchanged value of the record in its source topic
 */
public record JoinInput(Side  side, String originalKey, String value) {

    public enum Side {
        /** A success record of the parent connector: the parent exists in the target system. */
        PARENT,
        /** A success record of the parent connector without a parent ID. No child can be released by it. */
        PARENT_WITHOUT_PARENT_ID,
        /** A child record that must wait for its parent. */
        CHILD,
        /** A child record without a parent ID. It cannot be joined. */
        CHILD_WITHOUT_PARENT_ID
    }

    public static JoinInput  parent(String originalKey, String value) {
        return new JoinInput(Side.PARENT, originalKey, value);
    }

    public static JoinInput parentWithoutParentId(String originalKey, String value) {
        return new JoinInput(Side.PARENT_WITHOUT_PARENT_ID, originalKey, value);
    }

    public static JoinInput child(String originalKey, String value) {
        return new JoinInput(Side.CHILD, originalKey, value);
    }

    public static JoinInput childWithoutParentId(String originalKey, String value) {
        return new JoinInput(Side.CHILD_WITHOUT_PARENT_ID, originalKey, value);
    }
}
