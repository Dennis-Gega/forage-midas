package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;

    @Autowired
    public TransactionListener(
        UserRepository userRepository,
        TransactionRecordRepository transactionRecordRepository
    ) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")
    @Transactional
    public void listen(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(
            transaction.getRecipientId()
        );

        // Validation
        if (
            sender != null &&
            recipient != null &&
            sender.getBalance() >= transaction.getAmount()
        ) {
            // Adjust balances
            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(
                recipient.getBalance() + transaction.getAmount()
            );

            // Save updated users
            userRepository.save(sender);
            userRepository.save(recipient);

            // Record transaction
            TransactionRecord record = new TransactionRecord(
                sender,
                recipient,
                transaction.getAmount()
            );
            transactionRecordRepository.save(record);
        }
        // If invalid, do nothing (discard transaction)
    }
}
