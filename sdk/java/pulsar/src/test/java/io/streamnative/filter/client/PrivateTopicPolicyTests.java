package io.streamnative.filter.client;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PrivateTopicPolicyTests {

    @Test
    void defaultsAreAggressive() {
        PrivateTopicPolicy p = PrivateTopicPolicy.DEFAULT;
        assertThat(p.getMessageTtlSeconds()).isEqualTo(300);
        assertThat(p.getRetentionTimeMinutes()).isEqualTo(5);
        assertThat(p.getRetentionSizeMB()).isEqualTo(64);
        assertThat(p.getInactiveDeleteSeconds()).isEqualTo(600);
        assertThat(p.isDeleteOnClose()).isTrue();
        assertThat(p.isApplyPerTopicPolicies()).isTrue();
    }

    @Test
    void builderOverridesEachField() {
        PrivateTopicPolicy p = PrivateTopicPolicy.builder()
                .messageTtlSeconds(60)
                .retentionTimeMinutes(1)
                .retentionSizeMB(8)
                .inactiveDeleteSeconds(120)
                .deleteOnClose(false)
                .applyPerTopicPolicies(false)
                .build();
        assertThat(p.getMessageTtlSeconds()).isEqualTo(60);
        assertThat(p.getRetentionTimeMinutes()).isEqualTo(1);
        assertThat(p.getRetentionSizeMB()).isEqualTo(8);
        assertThat(p.getInactiveDeleteSeconds()).isEqualTo(120);
        assertThat(p.isDeleteOnClose()).isFalse();
        assertThat(p.isApplyPerTopicPolicies()).isFalse();
    }

    @Test
    void toBuilderRoundTrips() {
        PrivateTopicPolicy orig = PrivateTopicPolicy.builder()
                .messageTtlSeconds(42)
                .retentionTimeMinutes(7)
                .build();
        PrivateTopicPolicy copy = orig.toBuilder().build();
        assertThat(copy).isEqualTo(orig);
    }
}
