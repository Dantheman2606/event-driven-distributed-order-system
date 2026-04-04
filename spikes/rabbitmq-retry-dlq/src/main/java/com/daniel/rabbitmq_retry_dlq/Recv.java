package com.daniel.rabbitmq_retry_dlq;

import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class Recv {

    private static final String MAIN_EXCHANGE = "main.exchange";
    private static final String RETRY_EXCHANGE = "retry.exchange";
    private static final String DLX_EXCHANGE = "dlx.exchange";

    private static final String MAIN_QUEUE = "main.queue";
    private static final String RETRY_QUEUE = "retry.queue";
    private static final String DLQ_QUEUE = "dlq.queue";

    private static final String ROUTING_KEY = "task.created";

    private static final int MAX_RETRIES = 3;

    public static void main(String[] args) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        //EXCHANGE
        channel.exchangeDeclare(MAIN_EXCHANGE, BuiltinExchangeType.DIRECT, true);
        channel.exchangeDeclare(RETRY_EXCHANGE, BuiltinExchangeType.DIRECT, true);
        channel.exchangeDeclare(DLX_EXCHANGE, BuiltinExchangeType.DIRECT, true);

        // Create args for main queue to retry on failure
        Map<String, Object> mainArgs = new HashMap<>();
        mainArgs.put("x-dead-letter-exchange", RETRY_EXCHANGE);

        channel.queueDeclare(MAIN_QUEUE, true, false, false, mainArgs);
        channel.queueBind(MAIN_QUEUE, MAIN_EXCHANGE, ROUTING_KEY);

        // RETRY QUEUE
        Map<String, Object> retryArgs = new HashMap<>();
        retryArgs.put("x-message-ttl", 10000);
        retryArgs.put("x-dead-letter-exchange", MAIN_EXCHANGE);

        channel.queueDeclare(RETRY_QUEUE, true, false, false, retryArgs);
        channel.queueBind(RETRY_QUEUE, RETRY_EXCHANGE, ROUTING_KEY);

        // DLQ
        channel.queueDeclare(DLQ_QUEUE, true, false, false, null);
        channel.queueBind(DLQ_QUEUE, DLX_EXCHANGE, ROUTING_KEY);

        System.out.println("Waiting for messages...");

        // MAIN CONSUMER
        DeliverCallback mainCallback = (tag, delivery) -> {
          String msg = new String(delivery.getBody(), StandardCharsets.UTF_8);
          long deliveryTag = delivery.getEnvelope().getDeliveryTag();

          int retryCount = getRetryCount(delivery);
          System.out.println("Received: " + msg + " | retries: " + retryCount);

          try{
              throw new RuntimeException("Failed Intentionally");
          }
          catch (Exception e) {

              if(retryCount >= MAX_RETRIES) {
                  System.out.println("Sending to DLQ");

                  channel.basicPublish(DLX_EXCHANGE, ROUTING_KEY, delivery.getProperties(), delivery.getBody());

                  channel.basicAck(deliveryTag, false);
              }
              else {
                  System.out.println("↩ Retrying...");
                  channel.basicNack(deliveryTag, false, false);
              }
          }

        };
        channel.basicConsume(MAIN_QUEUE, false, mainCallback, tag -> {});

        // DLQ CONSUMER
        DeliverCallback dlqCallback = (tag, delivery) -> {
            String msg = new String(delivery.getBody(), StandardCharsets.UTF_8);

            System.out.println("Added to DLQ:" + msg);

            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };
        channel.basicConsume(DLQ_QUEUE, false, dlqCallback, tag -> {});


    }
    private static int getRetryCount(Delivery delivery) {
        Map<String, Object> headers = delivery.getProperties().getHeaders();

        if(headers == null || !headers.containsKey("x-death")) return 0;

        List<Map<String, Object>> deaths = (List<Map<String, Object>>) headers.get("x-death");

        return ((Long) deaths.get(0).get("count")).intValue();
    }
}
