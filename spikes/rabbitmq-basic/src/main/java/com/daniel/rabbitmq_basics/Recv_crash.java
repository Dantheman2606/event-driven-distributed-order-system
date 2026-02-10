package com.daniel.rabbitmq_basics;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class Recv_crash {
    private static final String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        System.out.println("Waiting for msgs, Press Ctrl + C to quit");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");

            System.out.println("Received msg: " + message);
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            System.out.println("Simulating system crash!");
            System.exit(1);

            // ack never sent, means the message will stay in the queue
            // channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        channel.basicQos(1);

        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });
    }
}
