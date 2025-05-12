package org.arpha.configuration;

import lombok.extern.slf4j.Slf4j;
import org.arpha.exception.ConfigurationException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

@Slf4j
public class ConfigurationManager {

    private static ConfigurationManager INSTANCE;
    private final Properties properties;

    private ConfigurationManager() {
        properties = new Properties();

        try (FileInputStream input = new FileInputStream("src/main/resources/config.properties")) {
            properties.load(input);
        } catch (FileNotFoundException e) {
            log.error("Configuration file not found: src/main/resources/config.properties", e);
            throw new ConfigurationException("Configuration file not found.", e);
        } catch (IOException e) {
            log.error("Error loading configuration file: src/main/resources/config.properties", e);
            throw new ConfigurationException("Error loading configuration file.", e);
        }
    }

    public static synchronized ConfigurationManager getINSTANCE() {
        if (INSTANCE == null) {
            INSTANCE = new ConfigurationManager();
        }

        return INSTANCE;
    }

    public String getProperty(String key, String defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) {
            log.warn("Property {} not found in configuration. Using default value: {}", key, defaultValue);
            return defaultValue;
        }
        return value;
    }

    public int getIntProperty(String key, int defaultValue) {
        String value = properties.getProperty(key);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                log.warn("Invalid integer format for property: {}", key, e);
                throw new ConfigurationException("Invalid integer format for property: " + key, e);
            }
        } else {
            log.info("Using default value for property: {}", key);
        }
        return defaultValue;
    }

}
