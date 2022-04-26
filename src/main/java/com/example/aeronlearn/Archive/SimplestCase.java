package com.example.aeronlearn.Archive;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @author : pan.han@okg.com
 * @date : 2022-04-25 22:55
 */
public class SimplestCase {

    private static final Logger logger = LoggerFactory.getLogger(SimplestCase.class);
    private final String channel = "aeron:ipc";
    private final int streamCapture = 16;
    private final int streamReplay = 17;
    //private final int sendCount = 10_000;
    private final int sendCount = 5;

    private final IdleStrategy idleStrategy = new SleepingIdleStrategy();
    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    private final File tempDir = Utils.createTempDir();
    boolean complete = false;
    private AeronArchive aeronArchive;
    private Aeron aeron;
    private ArchivingMediaDriver mediaDriver;


    public static void main(String[] args) {

        final SimplestCase archiveTest = new SimplestCase();

        //建立 Aeron 环境 -> setting up the Aeron environment;
        logger.info("Create Aeron Environment!");
        archiveTest.setUp();

        //写入 Archive -> writing the data to the Archive
        logger.info("Write Archive");
        archiveTest.write();

        //从 Archive 中读 -> reading the data from the archive
        logger.info("From Archive Read");
        archiveTest.read();

        //关闭 Aeron 相关  -> cleaning up
        archiveTest.cleanUp();

    }

    /**
     *
     */
    public void setUp() {
        mediaDriver = ArchivingMediaDriver.launch(
                new MediaDriver.Context()
                        //which instructs Aeron to create virtual subscribers (spies) on the publication.
                        //This ensures the publication can be written to without there being an attached subscriber.
                        .spiesSimulateConnection(true)
                        .dirDeleteOnStart(true),
                new Archive.Context()
                        .deleteArchiveOnStart(true)
                        //which tells Aeron Archive where to hold the archive data files.
                        .archiveDir(tempDir)
        );

        aeron = Aeron.connect();

        aeronArchive = AeronArchive.connect(
                new AeronArchive.Context()
                        .aeron(aeron)
        );
    }


    /**
     *  Publishing messages to the Archive
     *
     */
    private void write() {

        //instructs Aeron archive to start recording the given channel and stream on the local media driver.
        aeronArchive.startRecording(channel, streamCapture, SourceLocation.LOCAL);

        //creates the publication. Note that the stream and channel are identical in lines 102 and 106
        //-- this is because we're recording the same publication we write to.
        try (ExclusivePublication publication = aeron.addExclusivePublication(channel, streamCapture)) {

            while (!publication.isConnected()) {
                idleStrategy.idle();
            }

            for (int i = 0; i <= sendCount; i++) {
                buffer.putInt(0, i);
                while (publication.offer(buffer,0,Integer.BYTES) < 0){
                    idleStrategy.idle();
                }
            }

            //it waits until the archive data is fully written
            /*
                await the completion of the archive write process.
                This is done by first checking the last position of the publication,
                then waiting for the counter's current value for
                the same channel and stream to be equal to the last position of the publication.

                等待 archive 写入过程的完成。这是通过首先检查 pub 的最后位置，
                然后等待同一通道和流的计数器的当前值等于 pub 的最后位置来完成的。
             */
            final long stopPosition = publication.position();
            //1.这个 CounterReader 是干什么用的？
            final CountersReader countersReader = aeron.countersReader();
            //2.为什么要拿这两个位置作对比呢？
            final int counterId = RecordingPos.findCounterIdBySession(countersReader, publication.sessionId());
            while (countersReader.getCounterValue(counterId) < stopPosition){
                idleStrategy.idle();
            }
        }
    }


    /**
     *  Reading the Aeron archive
     *      1.Finding the recording id
     *          Before we can read the archive, we need to know which recording ID we want to read from.
     *          To do this, we need to list the available recordings and get the most recent one.
     *          Once we have the recording ID, we can replay the archive.
     *          为此，我们需要列出可用的录音并获取最新的录音。一旦我们有了记录 ID，我们就可以重播存档。
     *      2.Replaying the archive
     *          this starts the replay from the very beginning, and will stay reading the data until stopped.
     *          This allows a late joiner to get all the data missed at start,
     *          and to receive live data once caught up.
     *
     */
    private void read(){

        try(AeronArchive reader = AeronArchive.connect(new AeronArchive.Context().aeron(aeron))){

            final long recordingId = findLatestRecording(reader, channel, streamCapture);
            final long position = 0L;
            //sets up the length variable so that Aeron Archive knows to follow the live recording (aka Live Replay)
            final long length = Long.MAX_VALUE;

            //begins the replay
            final long sessionId = reader.startReplay(recordingId, position, length, channel, streamReplay);

            //connect a subscription to the replayed aeron archive.
            final String channelRead = ChannelUri.addSessionId(channel, (int) sessionId);
            final Subscription subscription = reader.context().aeron().addSubscription(channelRead, streamReplay);

            while (!subscription.isConnected()){
                idleStrategy.idle();
            }

            while (!complete){
                int fragments = subscription.poll(this::archiveReader, 1);
                idleStrategy.idle(fragments);
            }
        }
    }

    /**
     * 这是查找最近的还是最新的？
     * @param archive
     * @param channel
     * @param stream
     * @return
     */
    private long findLatestRecording(final AeronArchive archive,String channel,int stream){
        final MutableLong lastRecordingId = new MutableLong();

        //Consumer of events describing Aeron stream recordings.
        final RecordingDescriptorConsumer consumer =
                (controlSessionId, correlationId, recordingId,
                  startTimestamp, stopTimestamp, startPosition,
                  stopPosition, initialTermId, segmentFileLength,
                  termBufferLength, mtuLength, sessionId,
                  streamId, strippedChannel, originalChannel,
                  sourceIdentity) -> lastRecordingId.set(recordingId);

        final long fromRecordingId = 0L;
        final int recordCount = 100;

        final int foundCount = archive.listRecordingsForUri(fromRecordingId,recordCount,channel,stream,consumer);

        if (foundCount == 0){
            throw new IllegalStateException("no recording found");
        }

        return lastRecordingId.get();
    }


    public void archiveReader(DirectBuffer buffer, int offset, int length,Header header){
        final int valueRead = buffer.getInt(offset);
        logger.info("Received {}",valueRead);
        if (valueRead == sendCount){
            complete = true;
        }
    }

    /**
     *  When the application is ready to be exited,
     *  it is important to cleanly shutdown the archive,Aeron and the Media Driver.
     *
     *  Doing this in the incorrect order or not doing it may result in a hung process or core dump.
     *
     */
    private void cleanUp(){
        CloseHelper.quietClose(aeronArchive);
        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(mediaDriver);
    }
}
