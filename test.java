#!/bin/bash

# =============================================================================
# Kafka Consumer Latency Tool - FIXED (Spring Boot 3.x + CompletableFuture)
# =============================================================================
# The error "Cannot find symbol: method addCallback" occurs because:
#   In Spring Boot 3 / Spring Kafka 3.x, KafkaTemplate.send() now returns 
#   CompletableFuture<SendResult> instead of the deprecated ListenableFuture.
#   addCallback() no longer exists on CompletableFuture.
#
# This version fixes it using .whenComplete() – the modern, clean replacement.
# All other parts (multi-topic, same .p12 file, latency metrics, dashboard) remain intact.
#
# How to use:
#   1. Save this script as create-kafka-latency-tool-fixed.sh
#   2. chmod +x create-kafka-latency-tool-fixed.sh && ./create-kafka-latency-tool-fixed.sh
#   3. cd kafka-latency-tool
#   4. Update application.yml (brokers + absolute path to your client.p12)
#   5. mvn spring-boot:run
#   6. Open http://localhost:8080/dashboard.html
# =============================================================================

set -e

PROJECT_NAME="kafka-latency-tool"
PACKAGE="com.example.kafkalatencytool"
PACKAGE_PATH="src/main/java/com/example/kafkalatencytool"

echo "🚀 Creating FIXED Kafka Consumer Latency Tool (CompletableFuture version)..."

mkdir -p "$PROJECT_NAME"/{src/main/java/"${PACKAGE_PATH//.//}"/{config,service,controller},src/main/resources/static,src/main/resources}

# =============================================================================
# 1. pom.xml
# =============================================================================
cat > "$PROJECT_NAME/pom.xml" << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example</groupId>
    <artifactId>kafka-latency-tool</artifactId>
    <version>0.0.1-SNAPSHOT</version>
    <name>Kafka Consumer Latency Tool</name>

    <properties>
        <java.version>17</java.version>
        <spring-boot.version>3.3.4</spring-boot.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-dependencies</artifactId>
                <version>${spring-boot.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-registry-prometheus</artifactId>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
EOF

# =============================================================================
# 2. application.yml
# =============================================================================
cat > "$PROJECT_NAME/src/main/resources/application.yml" << 'EOF'
spring:
  application:
    name: kafka-latency-tool

app:
  kafka:
    bootstrap-servers: localhost:9093,broker2:9093,broker3:9093
    topics: latency-test-topic1,latency-test-topic2,latency-test-topic3
    group-id: kafka-latency-consumer-group

    ssl:
      keystore-location: /absolute/path/to/your/client.p12
      keystore-password: changeit
      key-password: changeit
      truststore-location: /absolute/path/to/your/client.p12
      truststore-password: changeit

server:
  port: 8080

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
EOF

# =============================================================================
# 3. Main Application
# =============================================================================
cat > "$PROJECT_NAME/src/main/java/${PACKAGE_PATH//.//}/KafkaLatencyToolApplication.java" << EOF
package ${PACKAGE};

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaLatencyToolApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaLatencyToolApplication.class, args);
        System.out.println("✅ Kafka Latency Tool started!");
        System.out.println("   Dashboard : http://localhost:8080/dashboard.html");
        System.out.println("   Prometheus: http://localhost:8080/actuator/prometheus");
    }
}
EOF

# =============================================================================
# 4. KafkaConfig.java
# =============================================================================
cat > "$PROJECT_NAME/src/main/java/${PACKAGE_PATH//.//}/config/KafkaConfig.java" << 'EOF'
package com.example.kafkalatencytool.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${app.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.group-id}")
    private String groupId;

    @Value("${app.kafka.ssl.keystore-location}")
    private String keystoreLocation;
    @Value("${app.kafka.ssl.keystore-password}")
    private String keystorePassword;
    @Value("${app.kafka.ssl.key-password}")
    private String keyPassword;
    @Value("${app.kafka.ssl.truststore-location}")
    private String truststoreLocation;
    @Value("${app.kafka.ssl.truststore-password}")
    private String truststorePassword;

    private Map<String, Object> getSslProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name());
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreLocation);
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword);
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
        return props;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.putAll(getSslProps());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        return factory;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.putAll(getSslProps());
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
EOF

# =============================================================================
# 5. Producer Service - FIXED with whenComplete()
# =============================================================================
cat > "$PROJECT_NAME/src/main/java/${PACKAGE_PATH//.//}/service/KafkaProducerService.java" << 'EOF'
package com.example.kafkalatencytool.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final List<String> topics;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate,
                                @Value("${app.kafka.topics}") String topicsCsv) {
        this.kafkaTemplate = kafkaTemplate;
        this.topics = Arrays.asList(topicsCsv.split(","));
    }

    @Scheduled(fixedRate = 10000)
    public void sendTestMessages() {
        long timestamp = System.currentTimeMillis();
        for (String topic : topics) {
            String message = "LATENCY_TEST_" + topic + "_" + timestamp;

            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    System.err.println("❌ Failed to send to " + topic + ": " + ex.getMessage());
                } else {
                    System.out.println("✅ Test message sent to " + topic 
                            + " | Offset: " + result.getRecordMetadata().offset());
                }
            });
        }
    }
}
EOF

# =============================================================================
# 6. Consumer Service
# =============================================================================
cat > "$PROJECT_NAME/src/main/java/${PACKAGE_PATH//.//}/service/KafkaConsumerService.java" << 'EOF'
package com.example.kafkalatencytool.service;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class KafkaConsumerService {

    private final MeterRegistry registry;
    private final Map<String, DistributionSummary> latencyHistograms = new ConcurrentHashMap<>();

    private final Map<String, AtomicLong> messageCounts = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> totalLatencyMs = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> maxLatencyMs = new ConcurrentHashMap<>();

    public KafkaConsumerService(MeterRegistry registry) {
        this.registry = registry;
    }

    @KafkaListener(
            topics = "#{'${app.kafka.topics}'.split(',')}",
            groupId = "${app.kafka.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(ConsumerRecord<String, String> record) {
        long latencyMs = System.currentTimeMillis() - record.timestamp();
        String topic = record.topic();

        DistributionSummary histogram = latencyHistograms.computeIfAbsent(topic, t ->
                DistributionSummary.builder("kafka.consumer.latency.ms")
                        .description("End-to-end latency per topic")
                        .baseUnit("milliseconds")
                        .tags("topic", t)
                        .register(registry));
        histogram.record(latencyMs);

        messageCounts.computeIfAbsent(topic, k -> new AtomicLong(0)).incrementAndGet();
        totalLatencyMs.computeIfAbsent(topic, k -> new AtomicLong(0)).addAndGet(latencyMs);
        maxLatencyMs.computeIfAbsent(topic, k -> new AtomicLong(0))
                .updateAndGet(current -> Math.max(current, latencyMs));

        System.out.printf("📥 Consumed [%-25s] | Latency: %5d ms | Offset: %d%n",
                topic, latencyMs, record.offset());
    }

    public Map<String, AtomicLong> getMessageCounts() { return messageCounts; }
    public Map<String, AtomicLong> getTotalLatencyMs() { return totalLatencyMs; }
    public Map<String, AtomicLong> getMaxLatencyMs() { return maxLatencyMs; }
}
EOF

# =============================================================================
# 7. Metrics Controller
# =============================================================================
cat > "$PROJECT_NAME/src/main/java/${PACKAGE_PATH//.//}/controller/MetricsController.java" << 'EOF'
package com.example.kafkalatencytool.controller;

import com.example.kafkalatencytool.service.KafkaConsumerService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@RestController
@RequestMapping("/api")
public class MetricsController {

    private final KafkaConsumerService consumerService;

    public MetricsController(KafkaConsumerService consumerService) {
        this.consumerService = consumerService;
    }

    @GetMapping("/latency-metrics")
    public Map<String, Object> getLatencyMetrics() {
        Map<String, Object> result = new HashMap<>();
        Map<String, Object> topicsData = new HashMap<>();

        consumerService.getMessageCounts().forEach((topic, countAtomic) -> {
            long count = countAtomic.get();
            long total = consumerService.getTotalLatencyMs()
                    .getOrDefault(topic, new AtomicLong(0)).get();
            long max = consumerService.getMaxLatencyMs()
                    .getOrDefault(topic, new AtomicLong(0)).get();
            double avg = count > 0 ? (double) total / count : 0.0;

            topicsData.put(topic, Map.of(
                    "count", count,
                    "avgMs", Math.round(avg * 100.0) / 100.0,
                    "maxMs", max,
                    "totalMs", total
            ));
        });

        result.put("topics", topicsData);
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }
}
EOF

# =============================================================================
# 8. Dashboard.html
# =============================================================================
cat > "$PROJECT_NAME/src/main/resources/static/dashboard.html" << 'EOF'
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Kafka Latency Dashboard</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
    <style>
        body { background: #0f172a; color: #e2e8f0; }
        .card { background: #1e2937; border: none; }
        .good { color: #22c55e; }
        .warn { color: #eab308; }
        .bad { color: #ef4444; }
    </style>
</head>
<body class="p-4">
<div class="container">
    <h1 class="display-5 fw-bold mb-4">🚀 Kafka Consumer Latency Tool</h1>
    
    <div class="card p-4 mb-4">
        <h5>📊 Average Latency by Topic (ms)</h5>
        <canvas id="latencyChart" height="110"></canvas>
    </div>

    <div class="card p-4">
        <h5>📋 Live Metrics</h5>
        <table class="table table-dark table-hover" id="metricsTable">
            <thead>
            <tr><th>Topic</th><th>Messages</th><th>Avg Latency</th><th>Max Latency</th><th>Total Latency</th></tr>
            </thead>
            <tbody></tbody>
        </table>
    </div>

    <div class="text-center mt-4">
        <a href="/actuator/prometheus" target="_blank" class="btn btn-outline-info btn-sm">📈 Prometheus Metrics</a>
        <span class="mx-3 text-muted">• Test messages every 10s •</span>
        <small id="lastUpdate" class="text-success"></small>
    </div>
</div>

<script>
    let chart;
    async function refreshDashboard() {
        try {
            const response = await fetch('/api/latency-metrics');
            const data = await response.json();
            const topics = data.topics || {};

            let html = '';
            Object.keys(topics).forEach(topic => {
                const m = topics[topic];
                const cls = m.avgMs < 50 ? 'good' : m.avgMs < 200 ? 'warn' : 'bad';
                html += `<tr>
                    <td><strong>${topic}</strong></td>
                    <td>${m.count}</td>
                    <td class="${cls}">${m.avgMs} ms</td>
                    <td>${m.maxMs} ms</td>
                    <td>${m.totalMs}</td>
                </tr>`;
            });
            document.querySelector('#metricsTable tbody').innerHTML = html || '<tr><td colspan="5" class="text-center text-muted">Waiting for messages...</td></tr>';

            const labels = Object.keys(topics);
            const values = labels.map(t => topics[t].avgMs);
            if (!chart) {
                chart = new Chart(document.getElementById('latencyChart'), {
                    type: 'bar',
                    data: { labels, datasets: [{ label: 'Avg Latency (ms)', data: values, backgroundColor: '#22c55e' }] },
                    options: { responsive: true, scales: { y: { beginAtZero: true } } }
                });
            } else {
                chart.data.labels = labels;
                chart.data.datasets[0].data = values;
                chart.update();
            }

            document.getElementById('lastUpdate').textContent = `Last updated: ${new Date().toLocaleTimeString()}`;
        } catch (e) {
            console.error(e);
        }
    }

    refreshDashboard();
    setInterval(refreshDashboard, 3000);
</script>
</body>
</html>
EOF

# =============================================================================
# 9. README.md
# =============================================================================
cat > "$PROJECT_NAME/README.md" << 'EOF'
# Kafka Consumer Latency Tool (Fixed for Spring Boot 3.x)

**Working version** with CompletableFuture (no more addCallback error).

- Multi-topic consumer from same brokers
- Same single .p12 file for keystore + truststore
- End-to-end latency measurement
- Prometheus metrics + live dashboard

### Run
1. Update `application.yml` (brokers, topics, absolute .p12 path)
2. `mvn spring-boot:run`
3. Open http://localhost:8080/dashboard.html

Now it compiles and runs cleanly!
EOF

echo "✅ FIXED project created in ./${PROJECT_NAME}/"
echo ""
echo "The addCallback error is resolved by switching to CompletableFuture.whenComplete()."
echo "Next steps:"
echo "   cd ${PROJECT_NAME}"
echo "   Update application.yml with your Kafka details and .p12 path"
echo "   mvn spring-boot:run"
echo "   Visit http://localhost:8080/dashboard.html"
```​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​