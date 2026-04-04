package com.daniel.rabbitmq_retry_dlq;

import com.rabbitmq.client.*;
import java.nio.charset.StandardCharsets;

public class Send {

    private static final String EXCHANGE = "main.exchange";
    private static final String ROUTING_KEY = "task.created";

    public static void main(String[] args) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try(Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()) {

            // Enable publisher confirms
            channel.confirmSelect();

            //Declare Exchange as the main.exchange
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT, true);

            // Enable Persistent messages - saved to disk
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .deliveryMode(2) // this is for persist
                    .contentType("text/plain")
                    .build();

            for(int i = 0; i < 5; i++) {
                String msg = "Message " + i;

                channel.basicPublish(EXCHANGE, ROUTING_KEY, props, msg.getBytes(StandardCharsets.UTF_8));

                System.out.println("Sent: "+ msg);
            }

            channel.waitForConfirmsOrDie(5000);
            System.out.println("All msgs confirmed");
        }
    }

}
