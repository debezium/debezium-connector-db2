/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Connector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

public class Db2ConnectorTest {

    Db2Connector connector;

    @BeforeEach
    void before() {
        connector = new Db2Connector();
    }

    @Test
    public void shouldReturnConfigurationDefinition() {
        assertConfigDefIsValid(connector, Db2ConnectorConfig.ALL_FIELDS);
    }

    @Test
    void testValidateUnableToConnectNoThrow() {
        Map<String, String> config = new HashMap<>();
        config.put(CommonConnectorConfig.TOPIC_PREFIX.name(), "dbserver1");
        config.put(RelationalDatabaseConnectorConfig.HOSTNAME.name(), "narnia");
        config.put(RelationalDatabaseConnectorConfig.PORT.name(), "4321");
        config.put(RelationalDatabaseConnectorConfig.DATABASE_NAME.name(), "db2inst1");
        config.put(RelationalDatabaseConnectorConfig.USER.name(), "pikachu");
        config.put(RelationalDatabaseConnectorConfig.PASSWORD.name(), "raichu");

        Config validated = connector.validate(config);
        ConfigValue hostName = getHostName(validated).orElseThrow(() -> new IllegalArgumentException("Host name config option not found"));
        assertThat(hostName.errorMessages().get(0)).startsWith("Unable to connect:");
    }

    @Test
    void testValidateMissingRequiredConfig() {
        Config validated = connector.validate(new HashMap<>());

        assertHasError(validated, CommonConnectorConfig.TOPIC_PREFIX.name());
        assertHasError(validated, RelationalDatabaseConnectorConfig.HOSTNAME.name());
        assertHasError(validated, RelationalDatabaseConnectorConfig.DATABASE_NAME.name());
    }

    private Optional<ConfigValue> getHostName(Config config) {
        return config.configValues()
                .stream()
                .filter(value -> value.name().equals(RelationalDatabaseConnectorConfig.HOSTNAME.name()))
                .findFirst();
    }

    private void assertHasError(Config config, String fieldName) {
        Optional<ConfigValue> configValue = config.configValues()
                .stream()
                .filter(value -> value.name().equals(fieldName))
                .findFirst();
        assertThat(configValue).isPresent();
        assertThat(configValue.get().errorMessages()).isNotEmpty();
    }

    protected static void assertConfigDefIsValid(Connector connector, io.debezium.config.Field.Set fields) {
        ConfigDef configDef = connector.config();
        assertThat(configDef).isNotNull();
        fields.forEach(expected -> {
            assertThat(configDef.names()).contains(expected.name());
            ConfigDef.ConfigKey key = configDef.configKeys().get(expected.name());
            assertThat(key).isNotNull();
            assertThat(key.name).isEqualTo(expected.name());
            assertThat(key.displayName).isEqualTo(expected.displayName());
            assertThat(key.importance).isEqualTo(expected.importance());
            assertThat(key.documentation).isEqualTo(expected.description());
            assertThat(key.type).isEqualTo(expected.type());
            if (expected.equals(Db2ConnectorConfig.SCHEMA_HISTORY)) {
                assertThat(((Class<?>) key.defaultValue).getName()).isEqualTo((String) expected.defaultValue());
            }
            assertThat(key.dependents).isEqualTo(expected.dependents());
            assertThat(key.width).isNotNull();
            assertThat(key.group).isNotNull();
            assertThat(key.orderInGroup).isGreaterThan(0);
            assertThat(key.validator).isNull();
            assertThat(key.recommender).isNull();
        });
    }

}
