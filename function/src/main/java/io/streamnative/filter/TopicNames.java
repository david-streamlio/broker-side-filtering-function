package io.streamnative.filter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TopicNames {

    private static final Pattern PARTITION_SUFFIX = Pattern.compile("-partition-\\d+$");

    private TopicNames() {}

    public static String normalize(String topic) {
        if (topic == null) {
            return null;
        }
        Matcher m = PARTITION_SUFFIX.matcher(topic);
        return m.find() ? topic.substring(0, m.start()) : topic;
    }
}
