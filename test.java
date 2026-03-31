#!/usr/bin/env bash
# =============================================================================
#  Kafka Consumer Latency Dashboard — Project Scaffold
#  Usage: chmod +x setup-kafka-dashboard.sh && ./setup-kafka-dashboard.sh
# =============================================================================
set -euo pipefail

PROJECT="kafka-latency-dashboard"
BASE="com/dashboard/kafka"
SRC="$PROJECT/src/main/java/$BASE"
RES="$PROJECT/src/main/resources"
TEST="$PROJECT/src/test/java/$BASE"

GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${CYAN}[INFO]${NC}  $1"; }
success() { echo -e "${GREEN}[OK]${NC}    $1"; }
header()  { echo -e "\n${YELLOW}━━━  $1  ━━━${NC}"; }

header "Creating project structure"

# ── Directories ───────────────────────────────────────────────────────────────
mkdir -p \
  "$SRC/config" \
  "$SRC/consumer" \
  "$SRC/metrics" \
  "$SRC/controller" \
  "$SRC/model" \
  "$RES/certs" \
  "$TEST/config" \
  "$TEST/consumer" \
  "$TEST/metrics" \
  "$PROJECT/dashboard" \
  "$PROJECT/docker"

info "Directories created"

# ══════════════════════════════════════════════════════════════════════════════
#  pom.xml
# ══════════════════════════════════════════════════════════════════════════════
header "Writing pom.xml"
cat << 'POMEOF' > "$PROJECT/pom.xml"
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.3.2</version>
        <relativePath/>
    </parent>

    <groupId>com.dashboard</groupId>
    <artifactId>kafka-latency-dashboard</artifactId>
    <version>1.0.0</version>
    <name>Kafka Consumer Latency Dashboard</name>
    <description>Production-ready Kafka consumer latency monitoring — multi-topic, SSL/P12, Micrometer/Prometheus</description>

    <properties>
        <java.version>21</java.version>
    </properties>

    <dependencies>
        <!-- Spring Boot Core -->
        <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-web</artifactId></dependency>
        <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-actuator</artifactId></dependency>
        <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-configuration-processor</artifactId><optional>true</optional></dependency>

        <!-- Kafka -->
        <dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka</artifactId></dependency>

        <!-- Micrometer + Prometheus -->
        <dependency><groupId>io.micrometer</groupId><artifactId>micrometer-registry-prometheus</artifactId></dependency>
        <dependency><groupId>io.micrometer</groupId><artifactId>micrometer-tracing-bridge-brave</artifactId></dependency>

        <!-- Resilience4j -->
        <dependency><groupId>io.github.resilience4j</groupId><artifactId>resilience4j-spring-boot3</artifactId><version>2.2.0</version></dependency>
        <dependency><groupId>io.github.resilience4j</groupId><artifactId>resilience4j-micrometer</artifactId><version>2.2.0</version></dependency>

        <!-- Lombok -->
        <dependency><groupId>org.projectlombok</groupId><artifactId>lombok</artifactId><optional>true</optional></dependency>

        <!-- Jackson -->
        <dependency><groupId>com.fasterxml.jackson.core</groupId><artifactId>jackson-databind</artifactId></dependency>
        <dependency><groupId>com.fasterxml.jackson.datatype</groupId><artifactId>jackson-datatype-jsr310</artifactId></dependency>

        <!-- Test -->
        <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-test</artifactId><scope>test</scope></dependency>
        <dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka-test</artifactId><scope>test</scope></dependency>
        <dependency><groupId>io.micrometer</groupId><artifactId>micrometer-test</artifactId><scope>test</scope></dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <configuration>
                    <excludes>
                        <exclude><groupId>org.projectlombok</groupId><artifactId>lombok</artifactId></exclude>
                    </excludes>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
POMEOF
success "pom.xml"

# ══════════════════════════════════════════════════════════════════════════════
#  application.yaml
# ══════════════════════════════════════════════════════════════════════════════
header "Writing application.yaml"
cat << 'YAMLEOF' > "$RES/application.yaml"
server:
  port: 8080

spring:
  application:
    name: kafka-latency-dashboard

  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:kafka-broker1:9093,kafka-broker2:9093}
    properties:
      security.protocol: SSL
      ssl.truststore.type: PKCS12
      ssl.keystore.type: PKCS12
      ssl.truststore.location: ${SSL_TRUSTSTORE_PATH:classpath:certs/truststore.p12}
      ssl.truststore.password: ${SSL_TRUSTSTORE_PASSWORD}
      ssl.keystore.location: ${SSL_KEYSTORE_PATH:classpath:certs/keystore.p12}
      ssl.keystore.password: ${SSL_KEYSTORE_PASSWORD}
      ssl.key.password: ${SSL_KEY_PASSWORD}
      reconnect.backoff.ms: 1000
      reconnect.backoff.max.ms: 10000
      retry.backoff.ms: 500
      request.timeout.ms: 30000

    consumer:
      group-id: ${CONSUMER_GROUP_ID:latency-dashboard-group}
      auto-offset-reset: latest
      enable-auto-commit: false
      max-poll-records: 500
      fetch-min-size: 1
      fetch-max-wait: 500ms
      heartbeat-interval: 3000ms
      session-timeout: 30000ms
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      properties:
        isolation.level: read_committed
        max.partition.fetch.bytes: 1048576

    listener:
      ack-mode: MANUAL_IMMEDIATE
      concurrency: 3
      poll-timeout: 3000ms
      missing-topics-fatal: false
      observation-enabled: true

kafka:
  topics:
    - name: orders
      group-id: orders-consumer-group
      partitions: 12
      latency-threshold-ms: 200
    - name: payments
      group-id: payments-consumer-group
      partitions: 6
      latency-threshold-ms: 100
    - name: notifications
      group-id: notifications-consumer-group
      partitions: 3
      latency-threshold-ms: 500
    - name: audit-events
      group-id: audit-consumer-group
      partitions: 6
      latency-threshold-ms: 1000
    - name: inventory-updates
      group-id: inventory-consumer-group
      partitions: 6
      latency-threshold-ms: 300
  latency-header: X-Produced-Timestamp
  admin:
    poll-interval-seconds: 10
    lag-alert-threshold: 10000

management:
  endpoints:
    web:
      exposure:
        include: health, info, prometheus, metrics
  endpoint:
    health:
      show-details: always
      probes:
        enabled: true
    prometheus:
      enabled: true
  metrics:
    tags:
      application: ${spring.application.name}
      environment: ${APP_ENV:production}
    distribution:
      percentiles-histogram:
        kafka.consumer.latency: true
      percentiles:
        kafka.consumer.latency: 0.5, 0.75, 0.90, 0.95, 0.99, 0.999
      slo:
        kafka.consumer.latency: 50ms, 100ms, 200ms, 500ms, 1s

logging:
  level:
    root: INFO
    com.dashboard.kafka: DEBUG
    org.apache.kafka: WARN
    org.springframework.kafka: INFO
  pattern:
    console: "%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level [%X{traceId},%X{spanId}] %logger{36} - %msg%n"
YAMLEOF
success "application.yaml"

# ══════════════════════════════════════════════════════════════════════════════
#  Java sources
# ══════════════════════════════════════════════════════════════════════════════
header "Writing Java sources"

# ── KafkaLatencyDashboardApplication.java ─────────────────────────────────────
cat << 'JAVAEOF' > "$SRC/KafkaLatencyDashboardApplication.java"
package com.dashboard.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties
public class KafkaLatencyDashboardApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaLatencyDashboardApplication.class, args);
    }
}
JAVAEOF
success "KafkaLatencyDashboardApplication.java"

# ── KafkaTopicProperties.java ──────────────────────────────────────────────────
cat << 'JAVAEOF' > "$SRC/config/KafkaTopicProperties.java"
package com.dashboard.kafka.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import java.util.List;

@Data
@Component
@ConfigurationProperties(prefix = "kafka")
public class KafkaTopicProperties {

    private List<TopicDefinition> topics;
    private String latencyHeader = "X-Produced-Timestamp";
    private AdminProperties admin = new AdminProperties();

    @Data
    public static class TopicDefinition {
        private String name;
        private String groupId;
        private int    partitions    = 1;
        private long   latencyThresholdMs = 500;
    }

    @Data
    public static class AdminProperties {
        private int  pollIntervalSeconds = 10;
        private long lagAlertThreshold   = 10_000;
    }
}
JAVAEOF
success "KafkaTopicProperties.java"

# ── KafkaConfig.java ───────────────────────────────────────────────────────────
cat << 'JAVAEOF' > "$SRC/config/KafkaConfig.java"
package com.dashboard.kafka.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@EnableKafka
@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")           private String bootstrapServers;
    @Value("${spring.kafka.properties.ssl.truststore.location}") private String truststoreLocation;
    @Value("${spring.kafka.properties.ssl.truststore.password}") private String truststorePassword;
    @Value("${spring.kafka.properties.ssl.keystore.location}")   private String keystoreLocation;
    @Value("${spring.kafka.properties.ssl.keystore.password}")   private String keystorePassword;
    @Value("${spring.kafka.properties.ssl.key.password}")        private String keyPassword;
    @Value("${spring.kafka.consumer.group-id}")          private String defaultGroupId;

    private Map<String, Object> baseConsumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3_000);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        // SSL / PKCS12
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.type", "PKCS12");
        props.put("ssl.keystore.type",   "PKCS12");
        props.put("ssl.truststore.location", truststoreLocation);
        props.put("ssl.truststore.password", truststorePassword);
        props.put("ssl.keystore.location",   keystoreLocation);
        props.put("ssl.keystore.password",   keystorePassword);
        props.put("ssl.key.password",        keyPassword);
        return props;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = baseConsumerProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, defaultGroupId);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    public ConsumerFactory<String, String> consumerFactoryForGroup(String groupId) {
        Map<String, Object> props = baseConsumerProps();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        return buildFactory(consumerFactory(), 3);
    }

    public ConcurrentKafkaListenerContainerFactory<String, String> buildFactory(
            ConsumerFactory<String, String> cf, int concurrency) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(cf);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setPollTimeout(3_000);
        factory.getContainerProperties().setObservationEnabled(true);
        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxInterval(60_000L);
        backOff.setMaxElapsedTime(600_000L);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(backOff);
        errorHandler.setRetryListeners((record, ex, attempt) ->
                log.warn("Retry #{} topic={} partition={} offset={}: {}",
                        attempt, record.topic(), record.partition(), record.offset(), ex.getMessage()));
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        cfg.put("security.protocol", "SSL");
        cfg.put("ssl.truststore.type", "PKCS12");
        cfg.put("ssl.keystore.type",   "PKCS12");
        cfg.put("ssl.truststore.location", truststoreLocation);
        cfg.put("ssl.truststore.password", truststorePassword);
        cfg.put("ssl.keystore.location",   keystoreLocation);
        cfg.put("ssl.keystore.password",   keystorePassword);
        cfg.put("ssl.key.password",        keyPassword);
        cfg.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        return new KafkaAdmin(cfg);
    }

    @Bean
    public AdminClient adminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }
}
JAVAEOF
success "KafkaConfig.java"

# ── LatencySnapshot.java ───────────────────────────────────────────────────────
cat << 'JAVAEOF' > "$SRC/metrics/LatencySnapshot.java"
package com.dashboard.kafka.metrics;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class LatencySnapshot {
    private String topic;
    private long   count;
    private double meanMs;
    private double p50Ms;
    private double p95Ms;
    private double p99Ms;
    private double maxMs;
    private double throughput;
    private double errors;
    private double sloViolations;
    private long   latencyThresholdMs;
}
JAVAEOF
success "LatencySnapshot.java"

# ── LatencyMetricsService.java ─────────────────────────────────────────────────
cat << 'JAVAEOF' > "$SRC/metrics/LatencyMetricsService.java"
package com.dashboard.kafka.metrics;

import com.dashboard.kafka.config.KafkaTopicProperties;
import io.micrometer.core.instrument.*;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Registers and updates all Kafka consumer latency metrics.
 *
 * Metrics:
 *   kafka_consumer_latency_seconds        — Timer histogram (p50/p95/p99/p999)
 *   kafka_consumer_throughput_total       — Counter (successful messages)
 *   kafka_consumer_errors_total           — Counter (processing failures)
 *   kafka_consumer_slo_violations_total   — Counter (p99 > threshold)
 *   kafka_consumer_batch_size             — DistributionSummary
 *   kafka_consumer_lag                    — Gauge (per partition)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LatencyMetricsService {

    private final MeterRegistry            meterRegistry;
    private final KafkaTopicProperties     topicProperties;

    private final Map<String, Timer>               latencyTimers        = new ConcurrentHashMap<>();
    private final Map<String, Counter>             throughputCounters   = new ConcurrentHashMap<>();
    private final Map<String, Counter>             errorCounters        = new ConcurrentHashMap<>();
    private final Map<String, Counter>             sloViolationCounters = new ConcurrentHashMap<>();
    private final Map<String, DistributionSummary> batchSizeMetrics     = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong>          lagValues            = new ConcurrentHashMap<>();

    @PostConstruct
    public void initMetrics() {
        if (topicProperties.getTopics() == null) return;
        topicProperties.getTopics().forEach(topic -> {
            String topicName = topic.getName();
            String groupId   = topic.getGroupId();
            Tags tags = Tags.of("topic", topicName, "group", groupId,
                                "environment", System.getProperty("spring.profiles.active", "production"));

            latencyTimers.put(topicName,
                Timer.builder("kafka.consumer.latency")
                     .description("End-to-end latency from produce to consume")
                     .tags(tags)
                     .publishPercentiles(0.5, 0.75, 0.90, 0.95, 0.99, 0.999)
                     .publishPercentileHistogram(true)
                     .serviceLevelObjectives(
                         java.time.Duration.ofMillis(50),  java.time.Duration.ofMillis(100),
                         java.time.Duration.ofMillis(200), java.time.Duration.ofMillis(500),
                         java.time.Duration.ofSeconds(1))
                     .register(meterRegistry));

            throughputCounters.put(topicName,
                Counter.builder("kafka.consumer.throughput")
                       .description("Total messages consumed successfully").tags(tags).register(meterRegistry));

            errorCounters.put(topicName,
                Counter.builder("kafka.consumer.errors")
                       .description("Total consumer processing errors").tags(tags).register(meterRegistry));

            sloViolationCounters.put(topicName,
                Counter.builder("kafka.consumer.slo.violations")
                       .description("Messages exceeding latency SLO")
                       .tags(tags.and("threshold_ms", String.valueOf(topic.getLatencyThresholdMs())))
                       .register(meterRegistry));

            batchSizeMetrics.put(topicName,
                DistributionSummary.builder("kafka.consumer.batch.size")
                       .description("Records per consumer poll batch").tags(tags).register(meterRegistry));

            for (int p = 0; p < topic.getPartitions(); p++) {
                String lagKey = topicName + ":" + p;
                AtomicLong lagValue = new AtomicLong(0);
                lagValues.put(lagKey, lagValue);
                final int partition = p;
                Gauge.builder("kafka.consumer.lag", lagValue, AtomicLong::get)
                     .description("Consumer lag per partition")
                     .tags(tags.and("partition", String.valueOf(partition)))
                     .register(meterRegistry);
            }
            log.info("Metrics registered for topic={} group={}", topicName, groupId);
        });
    }

    public void recordLatency(String topic, long producedEpochMs) {
        long latencyMs = System.currentTimeMillis() - producedEpochMs;
        Timer timer = latencyTimers.get(topic);
        if (timer != null) timer.record(latencyMs, TimeUnit.MILLISECONDS);
        KafkaTopicProperties.TopicDefinition def = findTopic(topic);
        if (def != null && latencyMs > def.getLatencyThresholdMs()) {
            Counter c = sloViolationCounters.get(topic);
            if (c != null) c.increment();
            log.warn("SLO VIOLATION topic={} latency={}ms threshold={}ms",
                    topic, latencyMs, def.getLatencyThresholdMs());
        }
    }

    public void recordLatency(String topic, Instant producedAt) {
        recordLatency(topic, producedAt.toEpochMilli());
    }

    public void recordThroughput(String topic, int count) {
        Counter c = throughputCounters.get(topic);
        if (c != null) c.increment(count);
    }

    public void recordError(String topic) {
        Counter c = errorCounters.get(topic);
        if (c != null) c.increment();
    }

    public void recordBatchSize(String topic, int batchSize) {
        DistributionSummary ds = batchSizeMetrics.get(topic);
        if (ds != null) ds.record(batchSize);
    }

    public void updateLag(String topic, int partition, long lag) {
        String key = topic + ":" + partition;
        AtomicLong lagValue = lagValues.computeIfAbsent(key, k -> {
            AtomicLong v = new AtomicLong(0);
            Gauge.builder("kafka.consumer.lag", v, AtomicLong::get)
                 .tags("topic", topic, "partition", String.valueOf(partition))
                 .register(meterRegistry);
            return v;
        });
        lagValue.set(lag);
    }

    public Map<String, LatencySnapshot> getLatencySnapshots() {
        Map<String, LatencySnapshot> snapshots = new LinkedHashMap<>();
        SimpleMeterRegistry noop = new SimpleMeterRegistry();
        latencyTimers.forEach((topic, timer) -> {
            KafkaTopicProperties.TopicDefinition def = findTopic(topic);
            long threshold = def != null ? def.getLatencyThresholdMs() : 500;
            snapshots.put(topic, LatencySnapshot.builder()
                    .topic(topic).count(timer.count())
                    .meanMs(timer.mean(TimeUnit.MILLISECONDS))
                    .p50Ms(timer.percentile(0.50, TimeUnit.MILLISECONDS))
                    .p95Ms(timer.percentile(0.95, TimeUnit.MILLISECONDS))
                    .p99Ms(timer.percentile(0.99, TimeUnit.MILLISECONDS))
                    .maxMs(timer.max(TimeUnit.MILLISECONDS))
                    .throughput(throughputCounters.getOrDefault(topic,
                            Counter.builder("noop").register(noop)).count())
                    .errors(errorCounters.getOrDefault(topic,
                            Counter.builder("noop").register(noop)).count())
                    .sloViolations(sloViolationCounters.getOrDefault(topic,
                            Counter.builder("noop").register(noop)).count())
                    .latencyThresholdMs(threshold).build());
        });
        return snapshots;
    }

    public long getTotalLag(String topic) {
        return lagValues.entrySet().stream()
                .filter(e -> e.getKey().startsWith(topic + ":"))
                .mapToLong(e -> e.getValue().get()).sum();
    }

    private KafkaTopicProperties.TopicDefinition findTopic(String name) {
        if (topicProperties.getTopics() == null) return null;
        return topicProperties.getTopics().stream()
                .filter(t -> t.getName().equals(name)).findFirst().orElse(null);
    }
}
JAVAEOF
success "LatencyMetricsService.java"

# ── ConsumerLagMonitor.java ────────────────────────────────────────────────────
cat << 'JAVAEOF' > "$SRC/metrics/ConsumerLagMonitor.java"
package com.dashboard.kafka.metrics;

import com.dashboard.kafka.config.KafkaTopicProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Polls AdminClient every {@code kafka.admin.poll-interval-seconds} seconds,
 * computes per-partition lag = endOffset - committedOffset, and pushes
 * values into Micrometer gauges via LatencyMetricsService.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ConsumerLagMonitor {

    private final AdminClient             adminClient;
    private final LatencyMetricsService   metricsService;
    private final KafkaTopicProperties    topicProperties;

    @Scheduled(fixedDelayString = "#{@kafkaTopicProperties.admin.pollIntervalSeconds * 1000}")
    public void pollConsumerLag() {
        if (topicProperties.getTopics() == null) return;
        topicProperties.getTopics().forEach(def -> {
            try {
                pollLagForGroup(def.getName(), def.getGroupId());
            } catch (Exception ex) {
                log.warn("Lag poll failed topic={} group={}: {}", def.getName(), def.getGroupId(), ex.getMessage());
            }
        });
    }

    private void pollLagForGroup(String topic, String groupId) throws Exception {
        ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(groupId);
        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committed =
                result.partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);

        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> topicOffsets =
                committed.entrySet().stream()
                        .filter(e -> e.getKey().topic().equals(topic))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (topicOffsets.isEmpty()) return;

        Map<TopicPartition, OffsetSpec> latestReq = topicOffsets.keySet().stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));

        Map<TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                adminClient.listOffsets(latestReq).all().get(10, TimeUnit.SECONDS);

        long totalLag = 0;
        for (Map.Entry<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> entry
                : topicOffsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            long committed2 = entry.getValue() != null ? entry.getValue().offset() : 0;
            long end        = endOffsets.containsKey(tp) ? endOffsets.get(tp).offset() : committed2;
            long lag        = Math.max(0, end - committed2);
            metricsService.updateLag(topic, tp.partition(), lag);
            totalLag += lag;
            if (lag > topicProperties.getAdmin().getLagAlertThreshold()) {
                log.warn("HIGH LAG topic={} partition={} lag={}", topic, tp.partition(), lag);
            }
        }
        log.debug("Lag polled topic={} group={} total={}", topic, groupId, totalLag);
    }
}
JAVAEOF
success "ConsumerLagMonitor.java"

# ── MultiTopicConsumer.java ────────────────────────────────────────────────────
cat << 'JAVAEOF' > "$SRC/consumer/MultiTopicConsumer.java"
package com.dashboard.kafka.consumer;

import com.dashboard.kafka.metrics.LatencyMetricsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Batch listeners for all configured topics.
 *
 * Each listener:
 *   1. Reads X-Produced-Timestamp header → computes end-to-end latency
 *   2. Records batch size, throughput, and errors via LatencyMetricsService
 *   3. ACKs only after all records in the batch succeed (at-least-once)
 *   4. Re-throws on failure → triggers exponential back-off retry
 *
 * Add your domain logic inside processRecord().
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MultiTopicConsumer {

    private final LatencyMetricsService metricsService;
    private static final String TS_HEADER = "X-Produced-Timestamp";

    @KafkaListener(topics = "#{@kafkaTopicProperties.topics[0].name}",
                   groupId = "#{@kafkaTopicProperties.topics[0].groupId}",
                   concurrency = "3", containerFactory = "kafkaListenerContainerFactory")
    public void consumeOrders(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        processBatch("orders", records, ack);
    }

    @KafkaListener(topics = "#{@kafkaTopicProperties.topics[1].name}",
                   groupId = "#{@kafkaTopicProperties.topics[1].groupId}",
                   concurrency = "2", containerFactory = "kafkaListenerContainerFactory")
    public void consumePayments(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        processBatch("payments", records, ack);
    }

    @KafkaListener(topics = "#{@kafkaTopicProperties.topics[2].name}",
                   groupId = "#{@kafkaTopicProperties.topics[2].groupId}",
                   concurrency = "1", containerFactory = "kafkaListenerContainerFactory")
    public void consumeNotifications(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        processBatch("notifications", records, ack);
    }

    @KafkaListener(topics = "#{@kafkaTopicProperties.topics[3].name}",
                   groupId = "#{@kafkaTopicProperties.topics[3].groupId}",
                   concurrency = "1", containerFactory = "kafkaListenerContainerFactory")
    public void consumeAuditEvents(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        processBatch("audit-events", records, ack);
    }

    @KafkaListener(topics = "#{@kafkaTopicProperties.topics[4].name}",
                   groupId = "#{@kafkaTopicProperties.topics[4].groupId}",
                   concurrency = "2", containerFactory = "kafkaListenerContainerFactory")
    public void consumeInventoryUpdates(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        processBatch("inventory-updates", records, ack);
    }

    // ── shared batch logic ────────────────────────────────────────────────────
    private void processBatch(String topic, List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        if (records.isEmpty()) { ack.acknowledge(); return; }
        metricsService.recordBatchSize(topic, records.size());
        int success = 0;
        for (ConsumerRecord<String, String> record : records) {
            try {
                long producedAt = extractProducedTimestamp(record);
                metricsService.recordLatency(topic, producedAt);
                processRecord(topic, record);
                success++;
            } catch (Exception ex) {
                metricsService.recordError(topic);
                log.error("Failed to process record topic={} partition={} offset={}: {}",
                        topic, record.partition(), record.offset(), ex.getMessage(), ex);
                throw new RuntimeException("Processing failed for topic=" + topic, ex);
            }
        }
        metricsService.recordThroughput(topic, success);
        ack.acknowledge();
        log.debug("Batch ACKed topic={} count={}", topic, success);
    }

    private long extractProducedTimestamp(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(TS_HEADER);
        if (header != null) {
            try { return Long.parseLong(new String(header.value(), StandardCharsets.UTF_8)); }
            catch (NumberFormatException ignored) {}
        }
        return record.timestamp();
    }

    /**
     * Replace with your domain service calls.
     * e.g. ordersService.handle(record.value());
     */
    private void processRecord(String topic, ConsumerRecord<String, String> record) {
        // TODO: dispatch to domain service based on topic
    }
}
JAVAEOF
success "MultiTopicConsumer.java"

# ── Model classes ──────────────────────────────────────────────────────────────
cat << 'JAVAEOF' > "$SRC/model/TopicSummary.java"
package com.dashboard.kafka.model;

import lombok.Builder;
import lombok.Data;

@Data @Builder
public class TopicSummary {
    private String  topic;
    private long    count;
    private double  meanMs, p50Ms, p95Ms, p99Ms, maxMs;
    private double  throughput, errors, sloViolations;
    private long    latencyThresholdMs, totalLag;
    private boolean sloBreached;
}
JAVAEOF

cat << 'JAVAEOF' > "$SRC/model/DashboardResponse.java"
package com.dashboard.kafka.model;

import lombok.Builder;
import lombok.Data;
import java.time.Instant;
import java.util.List;

@Data @Builder
public class DashboardResponse {
    private Instant           timestamp;
    private int               totalTopics;
    private int               sloBreaches;
    private List<TopicSummary> topics;
}
JAVAEOF
success "Model classes"

# ── DashboardController.java ───────────────────────────────────────────────────
cat << 'JAVAEOF' > "$SRC/controller/DashboardController.java"
package com.dashboard.kafka.controller;

import com.dashboard.kafka.config.KafkaTopicProperties;
import com.dashboard.kafka.metrics.LatencyMetricsService;
import com.dashboard.kafka.metrics.LatencySnapshot;
import com.dashboard.kafka.model.DashboardResponse;
import com.dashboard.kafka.model.TopicSummary;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * REST endpoints:
 *   GET /api/dashboard/metrics          — all topics
 *   GET /api/dashboard/metrics/{topic}  — single topic
 *   GET /api/dashboard/health           — HEALTHY / DEGRADED / CRITICAL
 */
@RestController
@RequestMapping("/api/dashboard")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class DashboardController {

    private final LatencyMetricsService metricsService;
    private final KafkaTopicProperties  topicProperties;

    @GetMapping("/metrics")
    public ResponseEntity<DashboardResponse> allMetrics() {
        Map<String, LatencySnapshot> snapshots = metricsService.getLatencySnapshots();
        List<TopicSummary> summaries = snapshots.values().stream()
                .map(this::toSummary).sorted(Comparator.comparing(TopicSummary::getTopic))
                .collect(Collectors.toList());
        return ResponseEntity.ok(DashboardResponse.builder()
                .timestamp(Instant.now()).topics(summaries)
                .totalTopics(summaries.size())
                .sloBreaches((int) summaries.stream().filter(TopicSummary::isSloBreached).count())
                .build());
    }

    @GetMapping("/metrics/{topic}")
    public ResponseEntity<TopicSummary> topicMetrics(@PathVariable String topic) {
        LatencySnapshot s = metricsService.getLatencySnapshots().get(topic);
        return s == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(toSummary(s));
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        long breaches = metricsService.getLatencySnapshots().values().stream()
                .filter(s -> s.getP99Ms() > s.getLatencyThresholdMs()).count();
        String status = breaches == 0 ? "HEALTHY" : breaches <= 2 ? "DEGRADED" : "CRITICAL";
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", status); body.put("timestamp", Instant.now());
        body.put("sloBreaches", breaches);
        return ResponseEntity.ok(body);
    }

    private TopicSummary toSummary(LatencySnapshot s) {
        return TopicSummary.builder()
                .topic(s.getTopic()).count(s.getCount())
                .meanMs(round(s.getMeanMs())).p50Ms(round(s.getP50Ms()))
                .p95Ms(round(s.getP95Ms())).p99Ms(round(s.getP99Ms()))
                .maxMs(round(s.getMaxMs())).throughput(s.getThroughput())
                .errors(s.getErrors()).sloViolations(s.getSloViolations())
                .latencyThresholdMs(s.getLatencyThresholdMs())
                .totalLag(metricsService.getTotalLag(s.getTopic()))
                .sloBreached(s.getP99Ms() > s.getLatencyThresholdMs()).build();
    }

    private double round(double v) { return Math.round(v * 100.0) / 100.0; }
}
JAVAEOF
success "DashboardController.java"

# ══════════════════════════════════════════════════════════════════════════════
#  dashboard/ — Prometheus + Grafana
# ══════════════════════════════════════════════════════════════════════════════
header "Writing dashboard configs"

cat << 'PROMEOF' > "$PROJECT/dashboard/prometheus.yml"
scrape_configs:
  - job_name: kafka-latency-dashboard
    scrape_interval: 10s
    scrape_timeout:  8s
    metrics_path:    /actuator/prometheus
    static_configs:
      - targets: [localhost:8080]
        labels:
          application: kafka-latency-dashboard
          environment: production
PROMEOF
success "prometheus.yml"

cat << 'GRAFEOF' > "$PROJECT/dashboard/grafana-dashboard.json"
{
  "title": "Kafka Consumer Latency",
  "uid":   "kafka-latency-v1",
  "tags":  ["kafka","consumer","latency","slo"],
  "time":  {"from":"now-1h","to":"now"},
  "refresh": "10s",
  "panels": [
    {"id":1,"title":"p99 Latency by topic (ms)","type":"timeseries","gridPos":{"x":0,"y":0,"w":12,"h":8},
     "targets":[{"expr":"histogram_quantile(0.99,sum by(topic,le)(rate(kafka_consumer_latency_seconds_bucket[2m])))*1000","legendFormat":"{{topic}}"}]},
    {"id":2,"title":"Throughput (msg/s)","type":"timeseries","gridPos":{"x":12,"y":0,"w":12,"h":8},
     "targets":[{"expr":"sum by(topic)(rate(kafka_consumer_throughput_total[1m]))","legendFormat":"{{topic}}"}]},
    {"id":3,"title":"Consumer lag per partition","type":"timeseries","gridPos":{"x":0,"y":8,"w":12,"h":8},
     "targets":[{"expr":"kafka_consumer_lag","legendFormat":"{{topic}}[{{partition}}]"}]},
    {"id":4,"title":"SLO violations","type":"stat","gridPos":{"x":12,"y":8,"w":12,"h":8},
     "targets":[{"expr":"sum by(topic)(kafka_consumer_slo_violations_total)","legendFormat":"{{topic}}"}]},
    {"id":5,"title":"Latency heatmap","type":"heatmap","gridPos":{"x":0,"y":16,"w":24,"h":10},
     "targets":[{"expr":"sum by(le)(rate(kafka_consumer_latency_seconds_bucket[5m]))","format":"heatmap","legendFormat":"{{le}}"}]}
  ]
}
GRAFEOF
success "grafana-dashboard.json"

# ══════════════════════════════════════════════════════════════════════════════
#  docker-compose.yml
# ══════════════════════════════════════════════════════════════════════════════
header "Writing docker-compose.yml"
cat << 'DCEOF' > "$PROJECT/docker-compose.yml"
version: "3.9"
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    environment: { ZOOKEEPER_CLIENT_PORT: 2181 }
    ports: ["2181:2181"]

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    depends_on: [zookeeper]
    ports: ["9092:9092", "9093:9093"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,SSL://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,SSL://localhost:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,SSL:SSL
      KAFKA_SSL_KEYSTORE_FILENAME: kafka.keystore.p12
      KAFKA_SSL_KEYSTORE_CREDENTIALS: kafka_keystore_creds
      KAFKA_SSL_KEY_CREDENTIALS: kafka_key_creds
      KAFKA_SSL_TRUSTSTORE_FILENAME: kafka.truststore.p12
      KAFKA_SSL_TRUSTSTORE_CREDENTIALS: kafka_truststore_creds
      KAFKA_SSL_CLIENT_AUTH: required
      KAFKA_SSL_KEYSTORE_TYPE: PKCS12
      KAFKA_SSL_TRUSTSTORE_TYPE: PKCS12
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    volumes: ["./src/main/resources/certs:/etc/kafka/secrets"]

  app:
    build: .
    depends_on: [kafka]
    ports: ["8080:8080"]
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9093
      SSL_TRUSTSTORE_PATH: /app/certs/truststore.p12
      SSL_KEYSTORE_PATH: /app/certs/keystore.p12
      SSL_TRUSTSTORE_PASSWORD: ${TRUSTSTORE_PASSWORD}
      SSL_KEYSTORE_PASSWORD: ${KEYSTORE_PASSWORD}
      SSL_KEY_PASSWORD: ${KEY_PASSWORD}
    volumes: ["./src/main/resources/certs:/app/certs:ro"]

  prometheus:
    image: prom/prometheus:v2.51.0
    ports: ["9090:9090"]
    volumes: ["./dashboard/prometheus.yml:/etc/prometheus/prometheus.yml:ro"]

  grafana:
    image: grafana/grafana:10.4.2
    ports: ["3000:3000"]
    depends_on: [prometheus]
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - grafana-data:/var/lib/grafana
      - ./dashboard/grafana-dashboard.json:/etc/grafana/provisioning/dashboards/kafka.json:ro

volumes:
  grafana-data:
DCEOF
success "docker-compose.yml"

# ══════════════════════════════════════════════════════════════════════════════
#  .env.example
# ══════════════════════════════════════════════════════════════════════════════
cat << 'ENVEOF' > "$PROJECT/.env.example"
KAFKA_BOOTSTRAP_SERVERS=your-broker:9093
SSL_TRUSTSTORE_PATH=/path/to/truststore.p12
SSL_TRUSTSTORE_PASSWORD=changeme
SSL_KEYSTORE_PATH=/path/to/keystore.p12
SSL_KEYSTORE_PASSWORD=changeme
SSL_KEY_PASSWORD=changeme
CONSUMER_GROUP_ID=latency-dashboard-group
APP_ENV=production
TRUSTSTORE_PASSWORD=changeme
KEYSTORE_PASSWORD=changeme
KEY_PASSWORD=changeme
ENVEOF
success ".env.example"

# ══════════════════════════════════════════════════════════════════════════════
#  .gitignore
# ══════════════════════════════════════════════════════════════════════════════
cat << 'GITEOF' > "$PROJECT/.gitignore"
target/
*.class
*.p12
*.jks
.env
*.log
.idea/
*.iml
.DS_Store
GITEOF
success ".gitignore"

# ══════════════════════════════════════════════════════════════════════════════
#  Print final tree
# ══════════════════════════════════════════════════════════════════════════════
header "Project structure"
find "$PROJECT" -not -path '*/target/*' | sort | awk '
{
  n = split($0, a, "/")
  indent = ""
  for (i=2; i<n; i++) indent = indent "│   "
  if (n > 1) {
    prefix = (NR==1) ? "" : indent "├── "
    print prefix a[n]
  } else {
    print a[n]
  }
}'

echo ""
success "All done! Your project is in ./$PROJECT"
echo ""
echo -e "  ${CYAN}Next steps:${NC}"
echo    "  1. Copy your .p12 files → $PROJECT/src/main/resources/certs/"
echo    "  2. cp $PROJECT/.env.example $PROJECT/.env  && fill in passwords"
echo    "  3. cd $PROJECT && mvn spring-boot:run"
echo    "     — or —"
echo    "     docker-compose up -d  (starts Kafka + app + Prometheus + Grafana)"
echo    "  4. Grafana → localhost:3000 → Import → dashboard/grafana-dashboard.json"
echo    "  5. Metrics → localhost:8080/actuator/prometheus"
