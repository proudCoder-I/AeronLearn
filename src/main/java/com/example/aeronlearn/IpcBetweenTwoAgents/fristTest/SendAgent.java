package com.example.aeronlearn.IpcBetweenTwoAgents.fristTest;

import io.aeron.Publication;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * @author : pan.han@okg.com
 * @date : 2022-04-25 14:56
 */
public class SendAgent  implements Agent {

    private final Publication publication;
    private final int sendCount;
    private final UnsafeBuffer unsafeBuffer;
    private int currentCountItem = 1;
    private final Logger logger = LoggerFactory.getLogger(SendAgent.class);


    public SendAgent(final Publication publication,int sendCount){
        this.publication = publication;
        this.sendCount = sendCount;
        this.unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocate(64));
        unsafeBuffer.putInt(0,currentCountItem);
    }


    @Override
    public int doWork() throws Exception {

        if (currentCountItem > sendCount){
            return 0;
        }

        if (publication.isConnected()){

            if(publication.offer(unsafeBuffer) > 0){
                currentCountItem += 1;
                unsafeBuffer.putInt(0,currentCountItem);
            }
        }
        return 0;
    }


    @Override
    public String roleName() {
        return "sender";
    }
}
