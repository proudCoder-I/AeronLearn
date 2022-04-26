package com.example.aeronlearn.IpcBetweenTwoAgents.fristTest;

import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author : pan.han@okg.com
 * @date : 2022-04-25 15:15
 */
public class ReceiveAgent implements Agent {

    private final Subscription subscription;
    private final ShutdownSignalBarrier barrier;
    private final int sendCount;
    private final Logger logger = LoggerFactory.getLogger(ReceiveAgent.class);

    public ReceiveAgent(final Subscription subscription,ShutdownSignalBarrier barrier,
                        int sendCount){
        this.subscription = subscription;
        this.barrier = barrier;
        this.sendCount = sendCount;
    }

    @Override
    public int doWork() throws Exception {
        subscription.poll(this::handler,1000);
        return 0;
    }

    // message -> UnsafeBuffer -> pub -> mediaDriver -> sub -> DirectBuffer -> message

    /**
     *
     * @param buffer
     * @param offset
     * @param length
     * @param header
     */
    private void handler(DirectBuffer buffer, int offset, int length, Header header){

        final int lastValue = buffer.getInt(offset);

        if (lastValue >= sendCount){
            logger.info("received: {}",lastValue);
            barrier.signal();
        }
    }

    @Override
    public String roleName() {
        return "receiver";
    }
}
