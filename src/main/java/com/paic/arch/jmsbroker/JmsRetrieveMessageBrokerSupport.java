package com.paic.arch.jmsbroker;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;

import javax.jms.*;
import java.lang.IllegalStateException;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Created by Ottack on 2018/3/1.
 * Retrieve
 */
public class JmsRetrieveMessageBrokerSupport {
    private static final Logger LOG = getLogger(JmsRetrieveMessageBrokerSupport.class);
    private static final int ONE_SECOND = 1000;
    private static final int DEFAULT_RECEIVE_TIMEOUT = 10 * ONE_SECOND;


    //取回消息操作
    private String executeCallbackAgainstRemoteBroker(String aBrokerUrl, String aDestinationName, JmsRetrieveMessageBrokerSupport.JmsCallback aCallback) {
        Connection connection = null;
        String returnValue = "";
        try {
            //创建一个链接工厂
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(aBrokerUrl);
            //从工厂中创建一个链接
            connection = connectionFactory.createConnection();
            //开启链接
            connection.start();
            //创建一个事务（这里通过参数可以设置事务的级别）
            returnValue = executeCallbackAgainstConnection(connection, aDestinationName, aCallback);
        } catch (JMSException jmse) {
            LOG.error("failed to create connection to {}", aBrokerUrl);
            throw new IllegalStateException(jmse);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException jmse) {
                    LOG.warn("Failed to close connection to broker at []", aBrokerUrl);
                    throw new IllegalStateException(jmse);
                }
            }
        }
        return returnValue;
    }

    interface JmsCallback {
        String performJmsFunction(Session aSession, Destination aDestination) throws JMSException;
    }

    //创建事务
    private String executeCallbackAgainstConnection(Connection aConnection, String aDestinationName, JmsRetrieveMessageBrokerSupport.JmsCallback aCallback) {
        Session session = null;
        try {
            session = aConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(aDestinationName);
            return aCallback.performJmsFunction(session, queue);
        } catch (JMSException jmse) {
            LOG.error("Failed to create session on connection {}", aConnection);
            throw new IllegalStateException(jmse);
        } finally {
            if (session != null) {
                try {
                    session.close();
                } catch (JMSException jmse) {
                    LOG.warn("Failed to close session {}", session);
                    throw new IllegalStateException(jmse);
                }
            }
        }
    }

    //取回消息
    public String retrieveASingleMessageFromTheDestination(String aDestinationName ,String brokerUrl) {
        return retrieveASingleMessageFromTheDestination(aDestinationName, DEFAULT_RECEIVE_TIMEOUT,brokerUrl);
    }

    //取回消息
    public String retrieveASingleMessageFromTheDestination(String aDestinationName, final int aTimeout,String brokerUrl) {
        return executeCallbackAgainstRemoteBroker(brokerUrl, aDestinationName, (aSession, aDestination) -> {
            //创建消费者
            MessageConsumer consumer = aSession.createConsumer(aDestination);
            //接收消息
            Message message = consumer.receive(aTimeout);
            if (message == null) {
                throw new JmsRetrieveMessageBrokerSupport.NoMessageReceivedException(String.format("No messages received from the broker within the %d timeout", aTimeout));
            }
            consumer.close();
            return ((TextMessage) message).getText();
        });
    }

    //无消息抛出异常
    public class NoMessageReceivedException extends RuntimeException {
        public NoMessageReceivedException(String reason) {
            super(reason);
        }
    }
}
