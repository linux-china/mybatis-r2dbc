package org.apache.ibatis.r2dbc.builder;

import org.apache.ibatis.builder.annotation.MethodResolver;

import java.lang.reflect.Method;

/**
 * @author Eduardo Macarron
 */
public class R2dbcMapperMethodResolver extends MethodResolver {

    private final R2dbcMapperAnnotationBuilder annotationBuilder;
    private final Method method;

    public R2dbcMapperMethodResolver(R2dbcMapperAnnotationBuilder annotationBuilder, Method method) {
        super(annotationBuilder, method);
        this.annotationBuilder = annotationBuilder;
        this.method = method;
    }


    @Override
    public void resolve() {
        annotationBuilder.parseStatement(method);
    }

}