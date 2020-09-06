package org.apache.ibatis.r2dbc;

import io.r2dbc.spi.ConnectionFactory;
import org.apache.ibatis.session.Configuration;

import java.io.Closeable;

/**
 * reactive SQL session factory
 *
 * @author linux_china
 */
public interface ReactiveSqlSessionFactory extends Closeable {

    ReactiveSqlSession openSession();

    Configuration getConfiguration();

    ConnectionFactory getConnectionFactory();
}
