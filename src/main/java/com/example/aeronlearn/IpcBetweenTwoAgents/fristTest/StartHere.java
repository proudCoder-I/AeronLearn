package com.example.aeronlearn.IpcBetweenTwoAgents.fristTest;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author : pan.han@okg.com
 * @date : 2022-04-25 15:49
 */
public class StartHere {

    private static final Logger logger  = LoggerFactory.getLogger(StartHere.class);


    public static void main(String[] args) {

        final String channel = "aeron:ipc";
        final int stream = 10;
        final int sendCount = 1_000_000;
        final IdleStrategy idleStrategySend = new BusySpinIdleStrategy();
        final IdleStrategy idleStrategyReceive = new BusySpinIdleStrategy();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();


        //
        final MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
                .dirDeleteOnStart(true)
                .threadingMode(ThreadingMode.SHARED)
                .sharedIdleStrategy(new BusySpinIdleStrategy())
                .dirDeleteOnShutdown(true);
        final MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaDriverCtx);


        //
        final Aeron.Context aeronCtx = new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName());
        final Aeron aeron = Aeron.connect(aeronCtx);


        //
        final Subscription subscription = aeron.addSubscription(channel,stream);
        final Publication publication = aeron.addPublication(channel, stream);


        //
        final SendAgent sendAgent = new SendAgent(publication,sendCount);
        final ReceiveAgent receiveAgent = new ReceiveAgent(subscription,barrier,sendCount);

        //
        final AgentRunner sendAgentRunner = new AgentRunner(idleStrategySend,
                Throwable::printStackTrace,null,sendAgent);
        final AgentRunner receiveAgentRunner = new AgentRunner(idleStrategyReceive,
                Throwable::printStackTrace,null,receiveAgent);

        //
        logger.info("starting");

        AgentRunner.startOnThread(sendAgentRunner);
        AgentRunner.startOnThread(receiveAgentRunner);

        barrier.await();


        receiveAgentRunner.close();
        sendAgentRunner.close();
        aeron.close();
        mediaDriver.close();
    }
}
