package org.apache.ibatis.r2dbc.spring.application.entity.extend;

import lombok.*;
import org.apache.ibatis.r2dbc.spring.application.entity.model.Dept;
import org.apache.ibatis.r2dbc.spring.application.entity.model.Emp;

/**
 * @author chenggang
 * @date 12/15/21.
 */
@ToString(callSuper = true)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class EmpWithDept extends Emp {

    private Dept dept;
}
