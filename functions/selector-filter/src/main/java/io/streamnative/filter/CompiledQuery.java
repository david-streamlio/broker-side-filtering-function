package io.streamnative.filter;

public record CompiledQuery(String queryId,
                            String name,
                            String outputTopic,
                            SelectorEvaluator evaluator) {
}
