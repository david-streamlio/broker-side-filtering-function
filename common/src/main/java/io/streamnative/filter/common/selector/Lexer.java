package io.streamnative.filter.common.selector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Hand-written scanner turning a selector expression into a {@link Token} stream. Recognizes JMS
 * string literals ({@code '...'} with doubled-quote escape), exact/approximate numerics, identifiers,
 * keywords (case-insensitive), and the operator/punctuation set. Unknown characters raise a
 * {@link SelectorParseException} carrying the offset.
 */
final class Lexer {

    private static final Map<String, Token.Type> KEYWORDS = Map.of(
            "AND", Token.Type.AND,
            "OR", Token.Type.OR,
            "NOT", Token.Type.NOT,
            "BETWEEN", Token.Type.BETWEEN,
            "LIKE", Token.Type.LIKE,
            "IN", Token.Type.IN,
            "IS", Token.Type.IS,
            "ESCAPE", Token.Type.ESCAPE);

    private final String src;
    private int pos;

    Lexer(String src) {
        this.src = src;
    }

    List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();
        Token t;
        do {
            t = next();
            tokens.add(t);
        } while (t.type() != Token.Type.EOF);
        return tokens;
    }

    private Token next() {
        skipWhitespace();
        if (pos >= src.length()) {
            return new Token(Token.Type.EOF, "", null, pos);
        }
        int start = pos;
        char c = src.charAt(pos);

        if (c == '\'') {
            return stringLiteral(start);
        }
        if (Character.isDigit(c) || (c == '.' && pos + 1 < src.length() && Character.isDigit(src.charAt(pos + 1)))) {
            return number(start);
        }
        if (isIdentStart(c)) {
            return identifierOrKeyword(start);
        }
        return operator(start);
    }

    private void skipWhitespace() {
        while (pos < src.length() && Character.isWhitespace(src.charAt(pos))) {
            pos++;
        }
    }

    private Token stringLiteral(int start) {
        pos++; // opening quote
        StringBuilder sb = new StringBuilder();
        while (pos < src.length()) {
            char c = src.charAt(pos);
            if (c == '\'') {
                if (pos + 1 < src.length() && src.charAt(pos + 1) == '\'') {
                    sb.append('\'');   // doubled-quote escape
                    pos += 2;
                    continue;
                }
                pos++; // closing quote
                return new Token(Token.Type.STRING, sb.toString(), sb.toString(), start);
            }
            sb.append(c);
            pos++;
        }
        throw new SelectorParseException("Unterminated string literal", start);
    }

    private Token number(int start) {
        boolean approximate = false;
        while (pos < src.length() && Character.isDigit(src.charAt(pos))) {
            pos++;
        }
        if (pos < src.length() && src.charAt(pos) == '.') {
            approximate = true;
            pos++;
            while (pos < src.length() && Character.isDigit(src.charAt(pos))) {
                pos++;
            }
        }
        if (pos < src.length() && (src.charAt(pos) == 'e' || src.charAt(pos) == 'E')) {
            approximate = true;
            pos++;
            if (pos < src.length() && (src.charAt(pos) == '+' || src.charAt(pos) == '-')) {
                pos++;
            }
            if (pos >= src.length() || !Character.isDigit(src.charAt(pos))) {
                throw new SelectorParseException("Malformed numeric exponent", start);
            }
            while (pos < src.length() && Character.isDigit(src.charAt(pos))) {
                pos++;
            }
        }
        String text = src.substring(start, pos);
        Object value = approximate ? (Object) Double.parseDouble(text) : (Object) Long.parseLong(text);
        return new Token(Token.Type.NUMBER, text, value, start);
    }

    private Token identifierOrKeyword(int start) {
        while (pos < src.length() && isIdentPart(src.charAt(pos))) {
            pos++;
        }
        String text = src.substring(start, pos);
        String upper = text.toUpperCase();
        if (upper.equals("TRUE")) {
            return new Token(Token.Type.BOOLEAN, text, Boolean.TRUE, start);
        }
        if (upper.equals("FALSE")) {
            return new Token(Token.Type.BOOLEAN, text, Boolean.FALSE, start);
        }
        if (upper.equals("NULL")) {
            return new Token(Token.Type.NULL, text, null, start);
        }
        Token.Type keyword = KEYWORDS.get(upper);
        if (keyword != null) {
            return new Token(keyword, text, null, start);
        }
        return new Token(Token.Type.IDENTIFIER, text, null, start);
    }

    private Token operator(int start) {
        char c = src.charAt(pos);
        switch (c) {
            case '=': pos++; return new Token(Token.Type.EQ, "=", null, start);
            case '+': pos++; return new Token(Token.Type.PLUS, "+", null, start);
            case '-': pos++; return new Token(Token.Type.MINUS, "-", null, start);
            case '*': pos++; return new Token(Token.Type.STAR, "*", null, start);
            case '/': pos++; return new Token(Token.Type.SLASH, "/", null, start);
            case '(': pos++; return new Token(Token.Type.LPAREN, "(", null, start);
            case ')': pos++; return new Token(Token.Type.RPAREN, ")", null, start);
            case ',': pos++; return new Token(Token.Type.COMMA, ",", null, start);
            case '<':
                pos++;
                if (peek() == '>') { pos++; return new Token(Token.Type.NEQ, "<>", null, start); }
                if (peek() == '=') { pos++; return new Token(Token.Type.LTE, "<=", null, start); }
                return new Token(Token.Type.LT, "<", null, start);
            case '>':
                pos++;
                if (peek() == '=') { pos++; return new Token(Token.Type.GTE, ">=", null, start); }
                return new Token(Token.Type.GT, ">", null, start);
            default:
                throw new SelectorParseException("Unexpected character '" + c + "'", start);
        }
    }

    private char peek() {
        return pos < src.length() ? src.charAt(pos) : '\0';
    }

    private static boolean isIdentStart(char c) {
        return Character.isLetter(c) || c == '_' || c == '$';
    }

    private static boolean isIdentPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$';
    }
}
