package com.example.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Reads the parent ID from a record.
 * With a JSON pointer (for example {@code /financialAccountId}) the ID comes from the JSON value.
 * Without a JSON pointer the ID is the record key.
 */
public class ParentIdExtractor {

    private static final Logger logger = LoggerFactory.getLogger(ParentIdExtractor.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Optional<String> jsonPointer;

    public ParentIdExtractor(Optional<String> jsonPointer) {
        this.jsonPointer = jsonPointer;
    }

    public Optional<String>  extract(String key, String value) {
        if (jsonPointer.isEmpty()) {
            return Optional.ofNullable(key).filter(k -> !k.isBlank());
        }
        if (value == null) {
            return Optional.empty();
        }
        try {
            JsonNode node = MAPPER.readTree(value).at(jsonPointer.get());
            if (node.isMissingNode() || node.isNull() || node.asText().isBlank()) {
                return Optional.empty();
            }
            return Optional.of(node.asText());
        } catch (JsonProcessingException e) {
            logger.warn("Value of record with key {} is not JSON: {}", key, e.getOriginalMessage());
            return Optional.empty();
        }
    }
}
