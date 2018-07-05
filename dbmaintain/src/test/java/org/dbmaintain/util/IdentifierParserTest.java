package org.dbmaintain.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class IdentifierParserTest {

    @Test
    void shouldParseSimpleIdentifierString() {
        final String simpleIdentifier = "myschema";
        final IdentifierParser identifierParser = new IdentifierParser('.');

        final String[] parsedIdentifier = identifierParser.parse(simpleIdentifier);

        assertArrayEquals(new String[] {simpleIdentifier}, parsedIdentifier);
    }

    @Test
    void shouldParseIdentifierWithSeparators() {
        final String identifier = "myschema.schema.my";
        final IdentifierParser identifierParser = new IdentifierParser('.');

        final String[] parsedIdentifier = identifierParser.parse(identifier);

        assertArrayEquals(new String[] {"myschema", "schema", "my"}, parsedIdentifier);
    }

    @ParameterizedTest
    @ValueSource(strings = {"'myschema'", "'myschema.schema'"})
    void shouldParseIdentifierWithSingelQuotes(String identifier) {
        final IdentifierParser identifierParser = new IdentifierParser('.');

        final String[] parsedIdentifier = identifierParser.parse(identifier);

        assertArrayEquals(new String[] {identifier}, parsedIdentifier);
    }

    @Test
    void shouldParseIdentifierWithSingelQuotesAndSeperator() {
        final String identifier = "'myschema.schema'.my";
        final IdentifierParser identifierParser = new IdentifierParser('.');

        final String[] parsedIdentifier = identifierParser.parse(identifier);

        assertArrayEquals(new String[] {"'myschema.schema'", "my"}, parsedIdentifier);
    }

    @ParameterizedTest
    @ValueSource(strings = {"\"myschema\"", "\"myschema.schema\""})
    void shouldParseIdentifierWithDoubleQuotes(String identifier) {
        final IdentifierParser identifierParser = new IdentifierParser('.');

        final String[] parsedIdentifier = identifierParser.parse(identifier);

        assertArrayEquals(new String[] {identifier}, parsedIdentifier);
    }

    @Test
    void shouldParseIdentifierWithDoubleQuotesAndSeperator() {
        final String identifier = "\"myschema.schema\".my";
        final IdentifierParser identifierParser = new IdentifierParser('.');

        final String[] parsedIdentifier = identifierParser.parse(identifier);

        assertArrayEquals(new String[] {"\"myschema.schema\"", "my"}, parsedIdentifier);
    }


}