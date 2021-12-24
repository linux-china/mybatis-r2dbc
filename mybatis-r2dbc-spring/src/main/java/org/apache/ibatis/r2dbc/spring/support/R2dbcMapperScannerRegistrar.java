package org.apache.ibatis.r2dbc.spring.support;

import org.mybatis.spring.annotation.MapperScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanNameGenerator;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;

/**
 * R2dbcMapperScannerRegistrar
 *
 * @author evans
 */
public class R2dbcMapperScannerRegistrar implements ImportBeanDefinitionRegistrar, ResourceLoaderAware {

    private static final Logger log = LoggerFactory.getLogger(R2dbcMapperScannerRegistrar.class);

    private ResourceLoader resourceLoader;

    @Override
    public void setResourceLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry, BeanNameGenerator importBeanNameGenerator) {
        this.registerBeanDefinitions(importingClassMetadata, registry);
    }

    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        AnnotationAttributes annoAttrs = AnnotationAttributes.fromMap(importingClassMetadata.getAnnotationAttributes(MapperScan.class.getName()));
        if (annoAttrs != null) {
            R2dbcClasspathMapperScanner scanner = new R2dbcClasspathMapperScanner(registry);

            if (resourceLoader != null) {
                scanner.setResourceLoader(resourceLoader);
            }

            Class<? extends Annotation> annotationClass = annoAttrs.getClass("annotationClass");
            if (!Annotation.class.equals(annotationClass)) {
                scanner.setAnnotationClass(annotationClass);
            }

            Class<? extends BeanNameGenerator> generatorClass = annoAttrs.getClass("nameGenerator");
            if (!BeanNameGenerator.class.equals(generatorClass)) {
                scanner.setBeanNameGenerator(BeanUtils.instantiateClass(generatorClass));
            }

            Class<? extends R2dbcMapperFactoryBean> mapperFactoryBeanClass = annoAttrs.getClass("factoryBean");
            if (!R2dbcMapperFactoryBean.class.equals(mapperFactoryBeanClass)) {
                scanner.setMapperFactoryBean(BeanUtils.instantiateClass(mapperFactoryBeanClass));
            }

            scanner.setSqlSessionFactoryBeanName(annoAttrs.getString("sqlSessionFactoryRef"));

            List<String> basePackages = new ArrayList<>();
            for (String pkg : annoAttrs.getStringArray("value")) {
                if (StringUtils.hasText(pkg)) {
                    basePackages.add(pkg);
                }
            }
            for (String pkg : annoAttrs.getStringArray("basePackages")) {
                if (StringUtils.hasText(pkg)) {
                    basePackages.add(pkg);
                }
            }
            for (Class<?> clazz : annoAttrs.getClassArray("basePackageClasses")) {
                basePackages.add(ClassUtils.getPackageName(clazz));
            }
            scanner.registerFilters();
            scanner.doScan(StringUtils.toStringArray(basePackages));
        } else {
            log.debug("MapperScan is not configured,default by scan with spring context");
        }
    }
}
