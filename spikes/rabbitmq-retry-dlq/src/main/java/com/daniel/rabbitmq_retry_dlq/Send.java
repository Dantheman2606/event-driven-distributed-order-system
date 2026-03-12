package com.daniel.rabbitmq_retry_dlq;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

public class Send {

    private static final String QUEUE_NAME = "Primary";
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();) {

            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            for(int i = 0; i < 5; i++) {
                String msg = "Message " + Integer.toString(i);

                channel.basicPublish("", QUEUE_NAME, null, msg.getBytes());
            }

            System.out.println("Messages published to Queue!");
        }
    }
}
