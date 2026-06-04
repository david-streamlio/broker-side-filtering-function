package io.streamnative.filter.common.selector;

/**
 * A lexical token. {@code literal} carries the typed value for {@link Type#STRING} ({@link String}),
 * {@link Type#NUMBER} ({@link Long} or {@link Double}), and {@link Type#BOOLEAN} ({@link Boolean});
 * it is {@code null} for all other token types.
 */
record Token(Token.Type type, String text, Object literal, int position) {

    enum Type {
        // literals
        STRING, NUMBER, BOOLEAN, NULL,
        // identifier
        IDENTIFIER,
        // keywords
        AND, OR, NOT, BETWEEN, LIKE, IN, IS, ESCAPE,
        // operators
        EQ, NEQ, LT, LTE, GT, GTE, PLUS, MINUS, STAR, SLASH,
        // punctuation
        LPAREN, RPAREN, COMMA,
        // end of input
        EOF
    }
}
