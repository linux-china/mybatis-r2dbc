package org.apache.ibatis.r2dbc.spring.application.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.r2dbc.spring.application.entity.model.Dept;
import org.apache.ibatis.r2dbc.spring.application.mapper.DeptMapper;
import org.apache.ibatis.r2dbc.spring.application.service.BusinessService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;


/**
 * @author: chenggang
 * @date 7/5/21.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BusinessServiceImpl implements BusinessService {

    private final DeptMapper deptMapper;

    @Transactional(rollbackFor = Exception.class)
    @Override
    public Mono<Dept> doWithTransactionBusiness() {
        return this.doBusinessInternal();
    }

    @Transactional(rollbackFor = Exception.class)
    @Override
    public Mono<Dept> doWithTransactionBusinessRollback() {
        return this.doBusinessInternal()
                .then(Mono.defer(() -> {
                    if (true) {
                        throw new RuntimeException("manually rollback with @Transaction");
                    }
                    return Mono.empty();
                }));
    }

    private Mono<Dept> doBusinessInternal() {
        return deptMapper.selectOneByDeptNo(4L)
                .doOnNext(people -> log.debug("[Before] Get People ,People:{}", people))
                .flatMap(people -> deptMapper.updateByDeptNo(new Dept()
                        .setDeptName("InsertDept")
                        .setLocation("InsertLocation")
                        .setCreateTime(LocalDateTime.now())
                        .setDeptNo(4L)
                ))
                .flatMap(value -> deptMapper.selectOneByDeptNo(4L))
                .doOnNext(updatePeople -> log.debug("[After Update] Get People ,People:{}", updatePeople))
                .flatMap(updatePeople -> deptMapper.deleteByDeptNo(4L))
                .flatMap(deleteResult -> deptMapper.selectOneByDeptNo(4L));
    }
}
