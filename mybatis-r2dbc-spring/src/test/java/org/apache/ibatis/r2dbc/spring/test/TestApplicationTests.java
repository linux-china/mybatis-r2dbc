package org.apache.ibatis.r2dbc.spring.test;

import org.apache.ibatis.r2dbc.spring.application.TestApplication;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

import java.util.UUID;

import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW;

@TestInstance(PER_CLASS)
@SpringBootTest(classes = TestApplication.class)
public class TestApplicationTests {

    @Autowired
    private ReactiveTransactionManager transactionManager;

    @BeforeAll
    public void contextLoads() {
        BlockHound.install();
        Hooks.onOperatorDebug();
        Hooks.enableContextLossTracking();
    }

    protected TransactionalOperator transactionalOperator() {
        DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setPropagationBehavior(PROPAGATION_REQUIRES_NEW);
        definition.setName(UUID.randomUUID().toString());
        return TransactionalOperator.create(transactionManager, definition);
    }

    public <T> Mono<T> withRollback(final Mono<T> publisher) {
        return this.transactionalOperator()
                .execute(tx -> {
                    tx.setRollbackOnly();
                    return publisher;
                })
                .singleOrEmpty();
    }

    public <T> Flux<T> withRollback(final Flux<T> publisher) {
        return this.transactionalOperator()
                .execute(tx -> {
                    tx.setRollbackOnly();
                    return publisher;
                });
    }

}

