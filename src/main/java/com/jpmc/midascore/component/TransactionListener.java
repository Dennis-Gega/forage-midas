package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    private final AtomicInteger counter = new AtomicInteger(0);

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")
    public void listen(Transaction transaction) {
    }
}
