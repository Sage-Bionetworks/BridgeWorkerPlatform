package org.sagebionetworks.bridge.udd.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.sagebionetworks.bridge.config.Config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@ComponentScan("org.sagebionetworks.bridge.udd")
@Configuration("uddConfig")
public class SpringConfig {
    @Autowired
    @Bean(name = "auxiliaryExecutorService")
    public ExecutorService auxiliaryExecutorService(Config config) {
        return Executors.newFixedThreadPool(config.getInt("threadpool.aux.count"));
    }
}
