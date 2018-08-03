package com.boot.rocketmq.single;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class Testing {
    /**
     * 生产者的组名
     */
    public static String PROCEDURE_GROUP = "PROCEDURE-GROUP";
    /**
     * 消费者组
     */
    public static String CONSUMER_GROUP = "CONSUMER_GROUP";
    /**
     * NameServer 地址
     */
    public static String NAMESERVERS = "127.0.0.1:9876";
    public static void main(String[] args)throws Exception{
        Thread thread1 = new Thread(new ConsumerThread(CONSUMER_GROUP+1));
        Thread thread2 = new Thread(new ConsumerThread(CONSUMER_GROUP+2));
        thread1.start();
        thread2.start();
        startProcedure();
        Thread.sleep(2000);
    }
    public static void startProcedure(){
        //生产者的组名
        DefaultMQProducer producer = new DefaultMQProducer(PROCEDURE_GROUP);
        DefaultMQProducer producer2 = new DefaultMQProducer(PROCEDURE_GROUP+2);
        //指定NameServer地址，多个地址以 ; 隔开
        producer.setNamesrvAddr(NAMESERVERS);
        producer2.setNamesrvAddr(NAMESERVERS);
        try {
            /**
             * Producer对象在使用之前必须要调用start初始化，初始化一次即可
             * 注意：切记不可以在每次发送消息时，都调用start方法
             */
            producer.start();
            producer2.start();
            for (int i = 0; i < 100; i++) {
                String messageBody = "我是消息内容:" + i;
                String message = new String(messageBody.getBytes(), "utf-8");
                //构建消息
                Message msg = new Message("TestTopic" /* PushTopic */, "push"/* Tag  */, "key_" + i /* Keys */, message.getBytes());
                //发送消息
                SendResult result = producer.send(msg);

                SendResult result2 = producer2.send(msg);
                System.out.println("producer 发送响应：MsgId:" + result.getMsgId() + "，发送状态:" + result.getSendStatus());
                System.out.println("producer2 发送响应：MsgId:" + result2.getMsgId() + "，发送状态:" + result2.getSendStatus());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.shutdown();
            producer2.shutdown();
        }
    }

    public static void startConsumer(){

    }

}
class ConsumerThread implements Runnable{
    private String consumerGroup;
    ConsumerThread(String consumerGroup){
        this.consumerGroup = consumerGroup;
    }
    @Override
    public void run() {

        //消费者的组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);

        //指定NameServer地址，多个地址以 ; 隔开
        consumer.setNamesrvAddr(Testing.NAMESERVERS);
        try {
            //订阅PushTopic下Tag为push的消息
            consumer.subscribe("TestTopic", "push");

            //设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
            //如果非第一次启动，那么按照上次消费的位置继续消费
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.registerMessageListener(new MessageListenerConcurrently() {

                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                    try {
                        for (MessageExt messageExt : list) {
                            System.out.println("ThreadName:"+Thread.currentThread().getName()+"===messageExt: " + messageExt);//输出消息内容
                            String messageBody = new String(messageExt.getBody(), "utf-8");
                            System.out.println("ThreadName:"+Thread.currentThread().getName()+"==="+"消费响应：Msg: " + messageExt.getMsgId() + ",msgBody: " + messageBody);//输出消息内容
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER; //稍后再试
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS; //消费成功
                }
            });
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}