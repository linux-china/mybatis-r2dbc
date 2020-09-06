package org.apache.ibatis.r2dbc.binding;

import org.apache.ibatis.MyBatisBaseTestSupport;
import org.apache.ibatis.r2dbc.demo.User;
import org.apache.ibatis.r2dbc.demo.UserMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * MethodSignature test
 *
 * @author linux_china
 */
public class MethodSignatureTest extends MyBatisBaseTestSupport {

    @Test
    public void testCreation() {
        MapperMethod.MethodSignature methodSignature = new MapperMethod.MethodSignature(getConfiguration(),
                UserMapper.class, findMethod(UserMapper.class, "findById"));
        assertThat(methodSignature.returnsMany()).isFalse();
        assertThat(methodSignature.getReturnInferredType()).isEqualTo(User.class);
    }
}
