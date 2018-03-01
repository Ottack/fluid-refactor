package com.paic.arch.jmsbroker;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JmsMessageBrokerSupportTest {

    public static final String TEST_QUEUE = "MY_TEST_QUEUE";
    public static final String MESSAGE_CONTENT = "Lorem blah blah";
    private static BrokerSupport JMS_SUPPORT;
    private static String REMOTE_BROKER_URL;

    @BeforeClass
    public static void setup() throws Exception {
        JMS_SUPPORT = BrokerSupport.createARunningEmbeddedBrokerOnAvailablePort();
        REMOTE_BROKER_URL = JMS_SUPPORT.getBrokerUrl();
    }

    @AfterClass
    public static void teardown() throws Exception {
        JMS_SUPPORT.stopTheRunningBroker();
    }

    @Test
    public void sendsMessagesToTheRunningBroker() throws Exception {
        BrokerSupport.bindToBrokerAtUrl(REMOTE_BROKER_URL)
                .andThen().sendATextMessageToDestinationAt(TEST_QUEUE, MESSAGE_CONTENT);
        long messageCount = JMS_SUPPORT.getEnqueuedMessageCountAt(TEST_QUEUE);
        assertThat(messageCount).isEqualTo(1);
    }

    @Test
    public void readsMessagesPreviouslyWrittenToAQueue() throws Exception {
        BrokerSupport.bindToBrokerAtUrl(REMOTE_BROKER_URL)
                .andThen().sendATextMessageToDestinationAt(TEST_QUEUE, MESSAGE_CONTENT);
        String receivedMessage = BrokerSupport.bindToBrokerAtUrl(REMOTE_BROKER_URL)
                .andThen().retrieveASingleMessageFromTheDestination(TEST_QUEUE);
        assertThat(receivedMessage).isEqualTo(MESSAGE_CONTENT);
    }

    @Test(expected = JmsRetrieveMessageBrokerSupport.NoMessageReceivedException.class)
    public void throwsExceptionWhenNoMessagesReceivedInTimeout() throws Exception {
        BrokerSupport.bindToBrokerAtUrl(REMOTE_BROKER_URL).retrieveASingleMessageFromTheDestination(TEST_QUEUE, 1);
    }
}