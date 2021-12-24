package org.apache.ibatis.r2dbc.spring.application.service;

import org.apache.ibatis.r2dbc.spring.application.entity.model.Dept;
import reactor.core.publisher.Mono;

/**
 * @author: chenggang
 * @date 7/5/21.
 */
public interface BusinessService {

    Mono<Dept> doWithTransactionBusiness();

    Mono<Dept> doWithTransactionBusinessRollback();

}
