package org.apache.ibatis.r2dbc.spring.test;

import org.apache.ibatis.r2dbc.spring.application.entity.extend.DeptWithEmp;
import org.apache.ibatis.r2dbc.spring.application.entity.model.Dept;
import org.apache.ibatis.r2dbc.spring.application.mapper.DeptMapper;
import org.apache.ibatis.r2dbc.spring.application.mapper.EmpMapper;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author evans
 */
public class DeptMapperTest extends TestApplicationTests {

    @Autowired
    private DeptMapper deptMapper;

    @Autowired
    private EmpMapper empMapper;

    @Test
    public void testGetDeptTotalCount() throws Exception {
        this.deptMapper.count()
                .as(StepVerifier::create)
                .expectNext(4L)
                .verifyComplete();
    }

    @Test
    public void testGetAllDept() throws Exception {
        this.deptMapper.selectAll()
                .as(StepVerifier::create)
                .expectNextCount(4)
                .verifyComplete();
    }

    @Test
    public void testGetDeptByDeptNo() throws Exception {
        Long deptNo = 1L;
        this.deptMapper.selectOneByDeptNo(deptNo)
                .as(StepVerifier::create)
                .expectNextMatches(dept -> deptNo.equals(dept.getDeptNo()))
                .verifyComplete();
    }

    @Test
    public void testGetDeptListByCreateTime() throws Exception {
        LocalDateTime createTime = LocalDateTime.now();
        this.deptMapper.selectListByTime(createTime)
                .as(StepVerifier::create)
                .thenConsumeWhile(result -> {
                    Assertions.assertThat(result)
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
        this.deptMapper.insert(dept)
                .as(this::withRollback)
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
        this.deptMapper.insert(dept)
                .then(Mono.defer(() -> deptMapper.deleteByDeptNo(dept.getDeptNo())))
                .as(this::withRollback)
                .as(StepVerifier::create)
                .expectNextMatches(effectRowCount -> effectRowCount == 1)
                .verifyComplete();
    }

    @Test
    public void testUpdateByDeptNo() throws Exception {
        Dept dept = new Dept();
        dept.setDeptNo(1L);
        dept.setDeptName("Update_dept_name");
        this.deptMapper.updateByDeptNo(dept)
                .as(this::withRollback)
                .as(StepVerifier::create)
                .expectNextMatches(effectRowCount -> effectRowCount == 1)
                .verifyComplete();
    }

    @Test
    public void testGetDeptWithEmp() throws Exception {
        this.deptMapper.selectDeptWithEmpList()
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
        this.empMapper.selectEmpWithDeptList()
                .as(StepVerifier::create)
                .thenConsumeWhile(empWithDept -> {
                    Assertions.assertThat(empWithDept.getDept()).isNotNull();
                    return true;
                })
                .verifyComplete();
    }

}
