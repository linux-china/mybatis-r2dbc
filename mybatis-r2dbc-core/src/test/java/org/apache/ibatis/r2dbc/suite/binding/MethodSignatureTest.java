package org.apache.ibatis.r2dbc.suite.binding;

import org.apache.ibatis.r2dbc.application.entity.extend.EmpWithDept;
import org.apache.ibatis.r2dbc.application.mapper.EmpMapper;
import org.apache.ibatis.r2dbc.binding.MapperMethod;
import org.apache.ibatis.r2dbc.suite.setup.MybatisR2dbcBaseTests;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MethodSignature test
 *
 * @author linux_china
 */
public class MethodSignatureTest extends MybatisR2dbcBaseTests {

    @Test
    public void testCreation() {
        MapperMethod.MethodSignature methodSignature = new MapperMethod.MethodSignature(r2dbcMybatisConfiguration,
                EmpMapper.class, findMethod(EmpMapper.class, "selectEmpWithDeptList"));
        assertThat(methodSignature.returnsMany()).isFalse();
        assertThat(methodSignature.getReturnInferredType()).isEqualTo(EmpWithDept.class);
    }

}
