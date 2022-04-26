package com.example.aeronlearn.AeronSimpleCase;

import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

/**
 * @author : pan.han@okg.com
 * @date : 2022-04-24 18:34
 */
public class SimplestCase {

    public static void main(String[] args) {

        //1.构建支持对象

        final int streamId = 10;
        //指定 Aeron 的 pub 和 sub 将通过其进行通信的通道
        final String channel = "aeron:ipc";
        //指定要通过 pub 提供的消息
        final String message = "my aeron basic message";
        //指定要使用的空闲策略，在这种情况下 SleepingIdleStrategy，将线程停顿 1 微妙，默认值
        final IdleStrategy idle = new SleepingIdleStrategy();
        //提供了一个堆外缓冲区，因此是不安全的缓冲区来保存要发送的消息。为简单起见，
        //分配了256个字节，但是，这可以减少。
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocate(256));

        //2.构建 Aeron 对象
        try(
                //创建 MediaDriver，它负责所有的 IPC 和 网络活动
                MediaDriver driver = MediaDriver.launch();
                //创建 Aeron 对象，这是应用程序中用于与 Aeron 交互的主要 API
                Aeron aeron = Aeron.connect();
                //创建将被轮询以接收消息的 sub
                Subscription sub = aeron.addSubscription(channel,streamId);
                //创建将发送消息的 pub
                Publication pub = aeron.addPublication(channel,streamId)
        ){
                // 48~55 负责发布消息。
            /*
                首先，应用程序等待 pub 达到连接状态。然后，将消息写入不安全缓冲区，
                最后将缓冲区提供给 pub 。
             */
            //仅在连接发布后，第53行才返回 true。这是在一个 while 循环中轮询的，迭代之间有
            //1微妙的暂停，直到它被连接。
                while (!pub.isConnected()){
                    //一微秒就是采用这个 SleepIdleStrategy ，它会将线程停顿 1 微秒
                    idle.idle();
                }
                unsafeBuffer.putStringAscii(0,message);
                System.out.println("sending:"+message);
                //其实我觉得应该是 pub 从缓冲区拿消息
                //如果没有消息的话，空闲策略就会再次轮询直到有消息被放入缓冲区。
                while (pub.offer(unsafeBuffer) < 0){
                    idle.idle();
                }

                // 67~72 用来接收消息
                /*
                首先我觉得是这样哈
                    Aeron 传递消息，然后 sub 接收消息，
                    接收到的消息 FragmentHandler 来处理
                 */
                FragmentHandler handler = ((buffer, offset, length, header) ->
                        System.out.println("received:"+buffer.getStringAscii(offset))
                    );
                //就是说你 sub poll 了消息后，然后小于 0 ，那么就说明 Aeron 还没有消息传过来
                while (sub.poll(handler,1) <= 0){
                    idle.idle();
                }
        }

    }
}
