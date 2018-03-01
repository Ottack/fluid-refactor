package com.paic.arch.jmsbroker;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.slf4j.Logger;
import java.lang.IllegalStateException;
import static org.slf4j.LoggerFactory.getLogger;
import static com.paic.arch.jmsbroker.SocketFinder.findNextAvailablePortBetween;



/**
 * Created by Ottack on 2018/3/1.
 * Broker
 */
public class BrokerSupport {
    private static final Logger LOG = getLogger(BrokerSupport.class);
    public static final String DEFAULT_BROKER_URL_PREFIX = "tcp://localhost:";

    private String brokerUrl;
    private BrokerService brokerService;

    private BrokerSupport(String aBrokerUrl) {
        brokerUrl = aBrokerUrl;
    }

    public static BrokerSupport createARunningEmbeddedBrokerOnAvailablePort() throws Exception {
        return createARunningEmbeddedBrokerAt(DEFAULT_BROKER_URL_PREFIX + findNextAvailablePortBetween(41616, 50000));
    }

    //使用代理
    public static BrokerSupport createARunningEmbeddedBrokerAt(String aBrokerUrl) throws Exception {
        LOG.debug("Creating a new broker at {}", aBrokerUrl);
        BrokerSupport broker = bindToBrokerAtUrl(aBrokerUrl);
        broker.createEmbeddedBroker();
        broker.startEmbeddedBroker();
        return broker;
    }

    //创建代理
    private void createEmbeddedBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.addConnector(brokerUrl);
    }

    //获取代理地址
    public static BrokerSupport bindToBrokerAtUrl(String aBrokerUrl) throws Exception {
        return new BrokerSupport(aBrokerUrl);
    }

    //开启代理
    private void startEmbeddedBroker() throws Exception {
        brokerService.start();
    }

    //停止代理
    public void stopTheRunningBroker() throws Exception {
        if (brokerService == null) {
            throw new IllegalStateException("Cannot stop the broker from this API: " +
                    "perhaps it was started independently from this utility");
        }
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    public final BrokerSupport andThen() {
        return this;
    }

    public final String getBrokerUrl() {
        return brokerUrl;
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

    //发送消息
    public JmsSendMessageBrokerSupport sendATextMessageToDestinationAt(String aDestinationName, final String aMessageToSend){
        JmsSendMessageBrokerSupport send = new JmsSendMessageBrokerSupport();
        return send.sendATextMessageToDestinationAt(aDestinationName,aMessageToSend,brokerUrl);
    }

    //取回消息
    public String retrieveASingleMessageFromTheDestination(String aDestinationName) {
        JmsRetrieveMessageBrokerSupport retrieve = new JmsRetrieveMessageBrokerSupport();
        return retrieve.retrieveASingleMessageFromTheDestination( aDestinationName , brokerUrl);
    }

    //取回消息
    public String retrieveASingleMessageFromTheDestination(String aDestinationName, final int aTimeout) {
        JmsRetrieveMessageBrokerSupport retrieve = new JmsRetrieveMessageBrokerSupport();
        return retrieve.retrieveASingleMessageFromTheDestination( aDestinationName ,aTimeout, brokerUrl);
    }
}
