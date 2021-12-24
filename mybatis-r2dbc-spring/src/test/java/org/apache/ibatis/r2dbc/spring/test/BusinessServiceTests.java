package org.apache.ibatis.r2dbc.spring.test;

import org.apache.ibatis.r2dbc.spring.application.service.BusinessService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.test.StepVerifier;

/**
 * @author: chenggang
 * @date 7/6/21.
 */
public class BusinessServiceTests extends TestApplicationTests {

    @Autowired
    private BusinessService businessService;

    @Test
    public void testDoWithTransactionBusiness() {
        businessService.doWithTransactionBusiness()
                .as(this::withRollback)
                .as(StepVerifier::create)
                .expectNextCount(0)
                .verifyComplete();
    }

    @Test
    public void testDoWithTransactionBusinessRollback() throws Exception {
        businessService.doWithTransactionBusinessRollback()
                .as(this::withRollback)
                .as(StepVerifier::create)
                .expectErrorMatches(throwable -> "manually rollback with @Transaction".equals(throwable.getMessage()))
                .verify();
    }
}
