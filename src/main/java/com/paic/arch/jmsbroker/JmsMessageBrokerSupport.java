package com.paic.arch.jmsbroker;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.slf4j.Logger;

import javax.jms.*;
import java.lang.IllegalStateException;

import static org.slf4j.LoggerFactory.getLogger;
import static com.paic.arch.jmsbroker.SocketFinder.findNextAvailablePortBetween;

public class JmsMessageBrokerSupport {
    private static final Logger LOG = getLogger(JmsMessageBrokerSupport.class);
    private static final int ONE_SECOND = 1000;
    private static final int DEFAULT_RECEIVE_TIMEOUT = 10 * ONE_SECOND;
    public static final String DEFAULT_BROKER_URL_PREFIX = "tcp://localhost:";

    private String brokerUrl;
    private BrokerService brokerService;

    private JmsMessageBrokerSupport(String aBrokerUrl) {
        brokerUrl = aBrokerUrl;
    }

    public static JmsMessageBrokerSupport createARunningEmbeddedBrokerOnAvailablePort() throws Exception {
        return createARunningEmbeddedBrokerAt(DEFAULT_BROKER_URL_PREFIX + findNextAvailablePortBetween(41616, 50000));
    }

    public static JmsMessageBrokerSupport createARunningEmbeddedBrokerAt(String aBrokerUrl) throws Exception {
        LOG.debug("Creating a new broker at {}", aBrokerUrl);
        JmsMessageBrokerSupport broker = bindToBrokerAtUrl(aBrokerUrl);
        broker.createEmbeddedBroker();
        broker.startEmbeddedBroker();
        return broker;
    }

    private void createEmbeddedBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.addConnector(brokerUrl);
    }

    public static JmsMessageBrokerSupport bindToBrokerAtUrl(String aBrokerUrl) throws Exception {
        return new JmsMessageBrokerSupport(aBrokerUrl);
    }

    private void startEmbeddedBroker() throws Exception {
        brokerService.start();
    }

    public void stopTheRunningBroker() throws Exception {
        if (brokerService == null) {
            throw new IllegalStateException("Cannot stop the broker from this API: " +
                    "perhaps it was started independently from this utility");
        }
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    public final JmsMessageBrokerSupport andThen() {
        return this;
    }

    public final String getBrokerUrl() {
        return brokerUrl;
    }

    //发送消息
    public JmsMessageBrokerSupport sendATextMessageToDestinationAt(String aDestinationName, final String aMessageToSend) {
        executeCallbackAgainstRemoteBroker(brokerUrl, aDestinationName, (aSession, aDestination) -> {
            //消息生产者
            MessageProducer producer = aSession.createProducer(aDestination);
            //发送消息
            producer.send(aSession.createTextMessage(aMessageToSend));
            producer.close();
            return "";
        });
        return this;
    }

    //取回消息
    public String retrieveASingleMessageFromTheDestination(String aDestinationName) {
        return retrieveASingleMessageFromTheDestination(aDestinationName, DEFAULT_RECEIVE_TIMEOUT);
    }

    //取回消息
    public String retrieveASingleMessageFromTheDestination(String aDestinationName, final int aTimeout) {
        return executeCallbackAgainstRemoteBroker(brokerUrl, aDestinationName, (aSession, aDestination) -> {
            //创建消费者
            MessageConsumer consumer = aSession.createConsumer(aDestination);
            //接收消息
            Message message = consumer.receive(aTimeout);
            if (message == null) {
                throw new NoMessageReceivedException(String.format("No messages received from the broker within the %d timeout", aTimeout));
            }
            consumer.close();
            return ((TextMessage) message).getText();
        });
    }

    //
    private String executeCallbackAgainstRemoteBroker(String aBrokerUrl, String aDestinationName, JmsCallback aCallback) {
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
    private String executeCallbackAgainstConnection(Connection aConnection, String aDestinationName, JmsCallback aCallback) {
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

    //获取目标消息长度
    public long getEnqueuedMessageCountAt(String aDestinationName) throws Exception {
        return getDestinationStatisticsFor(aDestinationName).getMessages().getCount();
    }

    //判断目标消息长度是否为空
    public boolean isEmptyQueueAt(String aDestinationName) throws Exception {
        return getEnqueuedMessageCountAt(aDestinationName) == 0;
    }

    //获取Broker的Destination
    private DestinationStatistics getDestinationStatisticsFor(String aDestinationName) throws Exception {
        Broker regionBroker = brokerService.getRegionBroker();
        for (org.apache.activemq.broker.region.Destination destination : regionBroker.getDestinationMap().values()) {
            if (destination.getName().equals(aDestinationName)) {
                return destination.getDestinationStatistics();
            }
        }
        throw new IllegalStateException(String.format("Destination %s does not exist on broker at %s", aDestinationName, brokerUrl));
    }

    //无消息抛出异常
    public class NoMessageReceivedException extends RuntimeException {
        public NoMessageReceivedException(String reason) {
            super(reason);
        }
    }
}
