package com.isums.paymentservice.configurations;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic quoteInvoiceCreateTopic() {
        return TopicBuilder.name("quote-invoice-create")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic quotePaymentCompletedTopic() {
        return TopicBuilder.name("quote-payment-completed")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic quoteCashPaymentConfirmedTopic() {
        return TopicBuilder.name("quote-cash-payment-confirmed")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
