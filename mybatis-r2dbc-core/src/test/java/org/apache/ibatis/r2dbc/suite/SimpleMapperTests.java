package org.apache.ibatis.r2dbc.suite;

import org.apache.ibatis.r2dbc.ReactiveSqlSession;
import org.apache.ibatis.r2dbc.ReactiveSqlSessionOperator;
import org.apache.ibatis.r2dbc.application.entity.extend.DeptWithEmp;
import org.apache.ibatis.r2dbc.application.entity.model.Dept;
import org.apache.ibatis.r2dbc.application.entity.model.Emp;
import org.apache.ibatis.r2dbc.application.mapper.DeptMapper;
import org.apache.ibatis.r2dbc.application.mapper.EmpMapper;
import org.apache.ibatis.r2dbc.defaults.DefaultReactiveSqlSessionOperator;
import org.apache.ibatis.r2dbc.suite.setup.MybatisR2dbcBaseTests;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author chenggang
 * @date 12/15/21.
 */
public class SimpleMapperTests extends MybatisR2dbcBaseTests {

    private ReactiveSqlSession reactiveSqlSession;
    private DeptMapper deptMapper;
    private EmpMapper empMapper;
    private ReactiveSqlSessionOperator reactiveSqlSessionOperator;

    @BeforeAll
    public void initSqlSession() throws Exception {
        this.reactiveSqlSession = super.reactiveSqlSessionFactory.openSession();
        this.reactiveSqlSessionOperator = new DefaultReactiveSqlSessionOperator(reactiveSqlSessionFactory);
        this.deptMapper = this.reactiveSqlSession.getMapper(DeptMapper.class);
        this.empMapper = this.reactiveSqlSession.getMapper(EmpMapper.class);
    }

    @Test
    public void testGetDeptTotalCount() throws Exception {
        reactiveSqlSessionOperator.execute(
                this.deptMapper.count()
        )
                .as(StepVerifier::create)
                .expectNext(4L)
                .verifyComplete();
    }

    @Test
    public void testGetAllDept() throws Exception {
        reactiveSqlSessionOperator.executeMany(
                this.deptMapper.selectAll()
        )
                .as(StepVerifier::create)
                .expectNextCount(4)
                .verifyComplete();
    }

    @Test
    public void testGetDeptByDeptNo() throws Exception {
        Long deptNo = 1L;
        this.reactiveSqlSessionOperator.execute(
                this.deptMapper.selectOneByDeptNo(deptNo)
        )
                .as(StepVerifier::create)
                .expectNextMatches(dept -> deptNo.equals(dept.getDeptNo()))
                .verifyComplete();
    }

    @Test
    public void testGetDeptListByCreateTime() throws Exception {
        LocalDateTime createTime = LocalDateTime.now();
        this.reactiveSqlSessionOperator.executeMany(
                this.deptMapper.selectListByTime(createTime)
        )
                .as(StepVerifier::create)
                .thenConsumeWhile(result -> {
                    assertThat(result)
                            .extracting(dept -> dept.getCreateTime().toLocalDate())
                            .matches(dateTime -> createTime.toLocalDate().equals(dateTime));
                    return true;
                })
                .verifyComplete();
    }

    @Test
    public void testInsertAndReturnGenerateKey() throws Exception {
        Dept dept = new Dept();
        dept.setDeptName("Test_dept_name");
        dept.setCreateTime(LocalDateTime.now());
        dept.setLocation("Test_location");
        reactiveSqlSessionOperator.executeAndRollback(this.deptMapper.insert(dept))
                .as(StepVerifier::create)
                .expectNextMatches(effectRowCount -> effectRowCount == 1)
                .verifyComplete();
        assertThat(dept.getDeptNo()).isNotNull();
    }

    @Test
    public void testDeleteByDeptNo() throws Exception {
        Dept dept = new Dept();
        dept.setDeptName("Test_dept_name");
        dept.setCreateTime(LocalDateTime.now());
        dept.setLocation("Test_location");
        reactiveSqlSessionOperator.executeAndRollback(
                this.deptMapper.insert(dept)
                        .then(Mono.defer(() -> this.deptMapper.deleteByDeptNo(dept.getDeptNo())))
        )
                .as(StepVerifier::create)
                .expectNextMatches(effectRowCount -> effectRowCount == 1)
                .verifyComplete();
    }

    @Test
    public void testUpdateByDeptNo() throws Exception {
        Dept dept = new Dept();
        dept.setDeptNo(1L);
        dept.setDeptName("Update_dept_name");
        reactiveSqlSessionOperator.executeAndRollback(this.deptMapper.updateByDeptNo(dept))
                .as(StepVerifier::create)
                .expectNextMatches(effectRowCount -> effectRowCount == 1)
                .verifyComplete();
    }

    @Test
    public void testGetDeptWithEmp() throws Exception {
        this.reactiveSqlSessionOperator.executeMany(
                this.deptMapper.selectDeptWithEmpList()
        )
                .as(StepVerifier::create)
                .expectNextMatches(deptWithEmp -> {
                    assertThat(deptWithEmp)
                            .extracting(DeptWithEmp::getEmpList)
                            .matches(empList -> empList.size() > 0);
                    return true;
                })
                .expectNextCount(2L)
                .expectNextMatches(deptWithEmp -> {
                    assertThat(deptWithEmp)
                            .extracting(DeptWithEmp::getEmpList)
                            .matches(empList -> empList.size() == 0);
                    return true;
                })
                .verifyComplete();
    }

    @Test
    public void testGetEmpWithDept() throws Exception {
        this.reactiveSqlSessionOperator.executeMany(
                this.empMapper.selectEmpWithDeptList()
        )
                .as(StepVerifier::create)
                .thenConsumeWhile(empWithDept -> {
                    assertThat(empWithDept.getDept()).isNotNull();
                    return true;
                })
                .verifyComplete();
    }

    @Test
    public void testGetEmpByParameterMap() throws Exception {
        Emp emp = new Emp();
        emp.setCreateTime(LocalDateTime.now());
        this.reactiveSqlSessionOperator.executeMany(
                this.empMapper.selectByParameterMap(emp)
        )
                .as(StepVerifier::create)
                .expectNextCount(14)
                .verifyComplete();
    }
}
