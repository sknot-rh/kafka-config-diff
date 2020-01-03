import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class DiffTest {
    String fileA = "broker.id=0\n" +
            "broker.rack=\n" +
            "\n" +
            "# Listeners\n" +
            "listeners=REPLICATION://0.0.0.0:9091,CLIENT://0.0.0.0:9092,CLIENTTLS://0.0.0.0:9093\n" +
            "advertised.listeners=REPLICATION://my-cluster-kafka-0.my-cluster-kafka-brokers.myproject.svc.cluster.local:9091,CLIENT://my-cluster-kafka-0.my-cluster-kafka-brokers.myproject.svc.cluster.local:9092,CLIENTTLS://my-cluster-kafka-0.my-cluster-kafka-brokers.myproject.svc.cluster.local:9093\n" +
            "listener.security.protocol.map=REPLICATION:SSL,CLIENT:PLAINTEXT,CLIENTTLS:SSL\n" +
            "inter.broker.listener.name=REPLICATION\n" +
            "sasl.enabled.mechanisms=\n" +
            "\n" +
            "# Zookeeper\n" +
            "zookeeper.connect=localhost:2181\n" +
            "\n" +
            "# Logs\n" +
            "log.dirs=/var/lib/kafka/data/kafka-log0\n" +
            "\n" +
            "# TLS / SSL\n" +
            "ssl.keystore.password=bZLoNnrC5Og87L79sSdRxFwALZ0VF3Qa\n" +
            "ssl.truststore.password=bZLoNnrC5Og87L79sSdRxFwALZ0VF3Qa\n" +
            "ssl.keystore.type=PKCS12\n" +
            "ssl.truststore.type=PKCS12\n" +
            "ssl.endpoint.identification.algorithm=HTTPS\n" +
            "ssl.secure.random.implementation=SHA1PRNG\n" +
            "\n" +
            "listener.name.replication.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12\n" +
            "listener.name.replication.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12\n" +
            "listener.name.replication.ssl.client.auth=required\n" +
            "\n" +
            "\n" +
            "# TLS interface configuration\n" +
            "listener.name.clienttls.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12\n" +
            "listener.name.clienttls.ssl.truststore.location=/tmp/kafka/clients.truststore.p12\n" +
            "# CLIENTTLS listener authentication\n" +
            "listener.name.clienttls.ssl.client.auth=none\n" +
            "\n" +
            "\n" +
            "# Authorization configuration\n" +
            "authorizer.class.name=\n" +
            "\n" +
            "\n" +
            "# Provided configuration\n" +
            "log.message.format.version=2.3\n" +
            "offsets.topic.replication.factor=3\n" +
            "transaction.state.log.min.isr=2\n" +
            "transaction.state.log.replication.factor=3";

    String fileB = "broker.id=1\n" +
            "broker.rack=\n" +
            "\n" +
            "# Listeners\n" +
            "listeners=REPLICATION://0.0.0.0:9091,CLIENT://0.0.0.0:9092,CLIENTTLS://0.0.0.0:9093\n" +
            "advertised.listeners=REPLICATION://my-cluster-kafka-0.my-cluster-kafka-brokers.myproject.svc.cluster.local:9091,CLIENT://my-cluster-kafka-0.my-cluster-kafka-brokers.myproject.svc.cluster.local:9092,CLIENTTLS://my-cluster-kafka-0.my-cluster-kafka-brokers.myproject.svc.cluster.local:9093\n" +
            "listener.security.protocol.map=REPLICATION:SSL,CLIENT:PLAINTEXT,CLIENTTLS:SSL\n" +
            "inter.broker.listener.name=REPLICATION\n" +
            "sasl.enabled.mechanisms=\n" +
            "\n" +
            "# Zookeeper\n" +
            "zookeeper.connect=localhost:2181\n" +
            "\n" +
            "# Logs\n" +
            "log.dirs=/var/lib/kafka/data/kafka-log0\n" +
            "\n" +
            "# TLS / SSL\n" +
            "ssl.keystore.password=bZLoNnrC5Og87L79sSdRxFwALZ0VF3Qa\n" +
            "ssl.truststore.password=bZLoNnrC5Og87L79sSdRxFwALZ0VF3Qa\n" +
            "ssl.keystore.type=PKCS12\n" +
            "ssl.truststore.type=PKCS12\n" +
            "ssl.endpoint.identification.algorithm=HTTPS\n" +
            "ssl.secure.random.implementation=SHA1PRNG\n" +
            "\n" +
            "listener.name.replication.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12\n" +
            "listener.name.replication.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12\n" +
            "listener.name.replication.ssl.client.auth=required\n" +
            "\n" +
            "\n" +
            "# TLS interface configuration\n" +
            "listener.name.clienttls.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12\n" +
            "listener.name.clienttls.ssl.truststore.location=/tmp/kafka/clients.truststore.p12\n" +
            "# CLIENTTLS listener authentication\n" +
            "listener.name.clienttls.ssl.client.auth=none\n" +
            "\n" +
            "\n" +
            "# Authorization configuration\n" +
            "authorizer.class.name=\n" +
            "\n" +
            "\n" +
            "# Provided configuration\n" +
            "log.message.format.version=2.3\n" +
            "offsets.topic.replication.factor=3\n" +
            "transaction.state.log.min.isr=2\n" +
            "transaction.state.log.replication.factor=3";


    @Test
    public void testDiff() {
       KafkaConfigurationDiff cd = new KafkaConfigurationDiff(fileA, fileB);
        assertTrue(cd.isDesiredConfigDynamicallyConfigurable());
    }

    @Test
    public void testDiffDesiredMissingOptionFromCurrent() {
        String fileC = "broker.id=0\n" +
                "broker.rack=\n" +
                "\n" +
                "# Listeners\n" +
                "listeners=REPLICATION://0.0.0.0:9091,CLIENT://0.0.0.0:9092,CLIENTTLS://0.0.0.0:9093";
        String fileD = "broker.id=0\n" +
                "broker.rack=\n" +
                "\n" +
                "# Listeners";
        KafkaConfigurationDiff cd = new KafkaConfigurationDiff(fileC, fileD);
        assertFalse(cd.isDesiredConfigDynamicallyConfigurable());
    }

    @Test
    public void testDiffCurrentMissingOptionFromDesired() {
        String fileC = "broker.id=0\n" +
                "broker.rack=\n" +
                "\n" +
                "# Listeners\n" +
                "listeners=REPLICATION://0.0.0.0:9091,CLIENT://0.0.0.0:9092,CLIENTTLS://0.0.0.0:9093";
        String fileD = "broker.id=0\n" +
                "broker.rack=\n" +
                "\n" +
                "# Listeners";
        KafkaConfigurationDiff cd = new KafkaConfigurationDiff(fileD, fileC);
        assertFalse(cd.isDesiredConfigDynamicallyConfigurable());
    }

    @Test
    public void testDiffCommentInCurrent() {
        String fileC = "broker.id=0\n" +
                "broker.rack=\n" +
                "\n" +
                "# Listeners\n" +
                "listeners=REPLICATION://0.0.0.0:9091,CLIENT://0.0.0.0:9092,CLIENTTLS://0.0.0.0:9093\n" +
                " #log.message.format.version=2.3\n";
        String fileD = "broker.id=0\n" +
                "broker.rack=\n" +
                "\n" +
                "# Listeners \n" +
                "listeners=REPLICATION://0.0.0.0:9091,CLIENT://0.0.0.0:9092,CLIENTTLS://0.0.0.0:9093";

        KafkaConfigurationDiff cd = new KafkaConfigurationDiff(fileC, fileD);
        assertEquals(cd.getOptionsWhichAreDifferent().size(), 0);
    }

    @Test
    public void testDiffCommentInDesired() {
        String fileC = "broker.id=0\n" +
                "broker.rack=\n" +
                "\n" +
                "# Listeners\n" +
                "listeners=REPLICATION://0.0.0.0:9091,CLIENT://0.0.0.0:9092,CLIENTTLS://0.0.0.0:9093";
        String fileD = "broker.id=0\n" +
                "broker.rack=\n" +
                "\n" +
                "# Listeners \n" +
                "listeners=REPLICATION://0.0.0.0:9091,CLIENT://0.0.0.0:9092,CLIENTTLS://0.0.0.0:9093\n" +
                " #log.message.format.version=2.3\n";
        KafkaConfigurationDiff cd = new KafkaConfigurationDiff(fileC, fileD);
        assertEquals(cd.getOptionsWhichAreDifferent().size(), 0);
    }
}
