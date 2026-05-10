package io.streamnative.filter;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TopicNamesTests {

    @Test
    void stripsPartitionSuffix() {
        assertThat(TopicNames.normalize("persistent://t/n/foo-partition-3"))
                .isEqualTo("persistent://t/n/foo");
    }

    @Test
    void leavesNonPartitionedAlone() {
        assertThat(TopicNames.normalize("persistent://t/n/foo"))
                .isEqualTo("persistent://t/n/foo");
    }

    @Test
    void leavesNonNumericPartitionAlone() {
        assertThat(TopicNames.normalize("persistent://t/n/foo-partition-bar"))
                .isEqualTo("persistent://t/n/foo-partition-bar");
    }

    @Test
    void nullSafe() {
        assertThat(TopicNames.normalize(null)).isNull();
    }

    @Test
    void preservesTenantNamespacePrefix() {
        assertThat(TopicNames.normalize("persistent://tenantA/nsB/topicC-partition-0"))
                .isEqualTo("persistent://tenantA/nsB/topicC");
    }
}
