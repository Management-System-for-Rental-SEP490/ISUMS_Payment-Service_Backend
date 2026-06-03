package com.isums.paymentservice.configurations;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic quoteInvoiceCreateTopic() {
        return singlePartitionTopic("quote-invoice-create");
    }

    @Bean
    public NewTopic quoteInvoiceCreateDltTopic() {
        return singlePartitionTopic("quote-invoice-create.DLT");
    }

    @Bean
    public NewTopic quotePaymentCompletedTopic() {
        return singlePartitionTopic("quote-payment-completed");
    }

    @Bean
    public NewTopic quoteCashPaymentConfirmedTopic() {
        return singlePartitionTopic("quote-cash-payment-confirmed");
    }

    @Bean
    public NewTopic quoteCashPaymentConfirmedDltTopic() {
        return singlePartitionTopic("quote-cash-payment-confirmed.DLT");
    }

    private NewTopic singlePartitionTopic(String name) {
        return TopicBuilder.name(name)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
