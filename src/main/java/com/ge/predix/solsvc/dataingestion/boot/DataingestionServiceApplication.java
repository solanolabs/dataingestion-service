package com.ge.predix.solsvc.dataingestion.boot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedServletContainerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;




/**
 * 
 * @author predix -
 */
@SpringBootApplication
@EnableAutoConfiguration
@ComponentScan(basePackages={"com.ge.predix.solsvc","com.ge.predix.solsvc.bootstrap.tsb"})
@PropertySource("classpath:application-default.properties")
public class DataingestionServiceApplication {
    /**
     * @param args -
     */
    public static void main(String[] args) {
        SpringApplication.run(DataingestionServiceApplication.class, args);
    }
    /**
     * @return -
     */
    @Bean
    public TomcatEmbeddedServletContainerFactory tomcatEmbeddedServletContainerFactory() {
        return new TomcatEmbeddedServletContainerFactory();
    }
    

}
