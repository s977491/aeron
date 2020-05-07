/*
 * Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.samples.archive;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.samples.SampleConfiguration;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SigInt;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.samples.SampleConfiguration.srcAeronDirectoryName;

/**
 * Basic Aeron publisher application which is recorded in an archive.
 * This publisher sends a fixed number of messages on a channel and stream ID.
 * <p>
 * The default values for number of messages, channel, and stream ID are
 * defined in {@link SampleConfiguration} and can be overridden by
 * setting their corresponding properties via the command-line; e.g.:
 * {@code -Daeron.sample.channel=aeron:udp?endpoint=localhost:5555 -Daeron.sample.streamId=20}
 */
public class ReplicatedBasicPublisher {
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final long NUMBER_OF_MESSAGES = SampleConfiguration.NUMBER_OF_MESSAGES;

    private static final UnsafeBuffer BUFFER = new UnsafeBuffer(BufferUtil.allocateDirectAligned(256, 64));
    private static final ChannelUriStringBuilder RECORDED_CHANNEL_BUILDER = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("239.255.255.255:3333");
//            .termLength(TERM_LENGTH);

    private static final String EXTEND_CHANNEL = new ChannelUriStringBuilder()
            .media("ipc")
//            .endpoint("localhost:3333")
            .build();
    private static int initialTermId;
    private static long stopPosition;
    private static int termBufferLength;
    private static int mtuLength;
    private static long recordingId;
    private static int sessionId;

    private static enum TestEnum {
        A(1), B(3);

        private final int i;

        public int getI() {
            return i;
        }

        TestEnum(int i) {
            this.i = i;
        }
    }
    private static <T extends Enum<T>>  T testFun(Class<T> testEnumClass, String b) {
        for (T enumConstant : testEnumClass.getEnumConstants()) {
            System.out.println(enumConstant.toString());
        }
        return Enum.valueOf(testEnumClass, b);
    }

    private static List<SignalHandler> handlers = new ArrayList<>();
    private static void register(SignalHandler runnable) {
        handlers.add(Signal.handle(new Signal("INT"), runnable));
    }

    public static void main(final String[] args) throws Exception {
        TestEnum a  = testFun(TestEnum.class, "B");
        System.out.println(a.toString());

        String CHANNEL = RECORDED_CHANNEL_BUILDER.build();
        System.out.println("Publishing to " + CHANNEL + " on stream id " + STREAM_ID);

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        try (Aeron aeron = Aeron.connect(
                new Aeron.Context()
                        .aeronDirectoryName(srcAeronDirectoryName))) {
            // Create a unique response stream id so not to clash with other archive clients.
            final AeronArchive.Context archiveCtx = new AeronArchive.Context()
                    .idleStrategy(YieldingIdleStrategy.INSTANCE)
                    .controlRequestChannel(SampleConfiguration.SRC_CONTROL_REQUEST_CHANNEL)
                    .controlResponseChannel(SampleConfiguration.SRC_CONTROL_RESPONSE_CHANNEL)
                    .aeron(aeron);
            RecordingSignalMonitor recordingSignalMonitor = new RecordingSignalMonitor();
            try (AeronArchive archive = AeronArchive.connect(archiveCtx)) {


//            archive.purgeSegments(0, 1 << 17);
                long recordingSubId;

                boolean newRecording = false;
//            if (count > 0) {
                final CountersReader counters = archive.context().aeron().countersReader();
                int counterIdBySession = RecordingPos.findCounterIdBySession(counters, sessionId);
                System.out.println("counterIdBySession:" + counterIdBySession);

                int counterIdByRecordId = recordingSignalMonitor.getCounterIdByRecordId(archive, 0);
                System.out.println("counterIdByRecordId:" + counterIdByRecordId);
                ;
                if (counterIdByRecordId != CountersReader.NULL_COUNTER_ID) {
                    while (RecordingPos.isActive(counters, counterIdByRecordId, 0)) {
                        System.out.println("Waiting...");
                        Thread.sleep(100);
                    }
                }
                int count = updateWithLatestRecording(CHANNEL, archive);
                if (count > 0) {
                    System.out.println("extending");
                    CHANNEL = RECORDED_CHANNEL_BUILDER.sessionId(sessionId).initialPosition(stopPosition, initialTermId, termBufferLength)
                                                      .mtu(mtuLength)
                                                      .build();
                    try {
                        recordingSubId = archive.extendRecording(recordingId, CHANNEL, STREAM_ID, LOCAL);
                    } catch (Exception e) {
                        //already replicated... just go on.
                        System.out.println("Already replicating, may just continue publish ");
                    }

                } else {
                    System.out.println("Start new recording!");
                    recordingSubId = archive.startRecording(CHANNEL, STREAM_ID, SourceLocation.REMOTE, true);
                    newRecording = true;
                }

                try (Publication publication = archive.context().aeron().addExclusivePublication(CHANNEL, STREAM_ID)) {
                    System.out.println("publisher session id:" + publication.sessionId());
                    final IdleStrategy idleStrategy = YieldingIdleStrategy.INSTANCE;
                    // Wait for recording to have started before publishing.
                    if (newRecording) {
                        int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
                        while (CountersReader.NULL_COUNTER_ID == counterId) {
                            if (!running.get()) {
                                return;
                            }

                            idleStrategy.idle();
                            counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
                        }

                        recordingId = RecordingPos.getRecordingId(counters, counterId);
                    }
                    System.out.println("Recording started: recordingId = " + recordingId);
                    int i = 0;
                    for (; i < NUMBER_OF_MESSAGES * 1000 && running.get(); i++) {
                        final String message = "Hello World! " + i;
                        final byte[] messageBytes = message.getBytes();
                        BUFFER.putBytes(0, messageBytes);

                        System.out.print(publication.position() + "Offering " + i + "/" + NUMBER_OF_MESSAGES + " - ");

                        final long result = publication.offer(BUFFER, 0, messageBytes.length);
                        checkResult(result);

                        final String errorMessage = archive.pollForErrorResponse();
                        if (null != errorMessage) {
                            throw new IllegalStateException(errorMessage);
                        }

//                    Thread.sleep
//                    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                        Thread.sleep(500);
                    }
                    System.out.print("Offering " + i + "/" + NUMBER_OF_MESSAGES + " - ");

                    idleStrategy.reset();
//                while (counters.getCounterValue(counterId) < publication.position()) {
//                    if (!RecordingPos.isActive(counters, counterId, recordingId)) {
//                        throw new IllegalStateException("recording has stopped unexpectedly: " + recordingId);
//                    }
//
//                    idleStrategy.idle();
//                }
                } finally {
                    System.out.println("Done sending.");
                    archive.stopRecording(CHANNEL, STREAM_ID);
                }
            }

        }
    }

    private static int updateWithLatestRecording(String CHANNEL, AeronArchive archive) {
        return findLatestRecording(archive, CHANNEL, STREAM_ID,
                                   (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                                    startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                                    mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                                       if (ReplicatedBasicPublisher.stopPosition < stopPosition) {
                                           ReplicatedBasicPublisher.initialTermId = initialTermId;
                                           ReplicatedBasicPublisher.stopPosition = stopPosition;
                                           ReplicatedBasicPublisher.termBufferLength = termBufferLength;
                                           ReplicatedBasicPublisher.mtuLength = mtuLength;
                                           ReplicatedBasicPublisher.recordingId = recordingId;
                                           ReplicatedBasicPublisher.sessionId = sessionId;
                                           System.out.println("sessionId:" + sessionId);
                                           System.out.println("streamId:" + streamId);
                                           System.out.println("recordingId:" + recordingId);
                                           System.out.println("startPosition:" + startPosition);
                                           System.out.println("stopPosition:" + stopPosition);
                                           System.out.println("originalChannel:" + originalChannel);
                                           System.out.println("sourceIdentity:" + sourceIdentity);

                                       }
                                   });
    }

    private static void checkResult(final long result) {
        if (result > 0) {
            System.out.println("yay!");
        } else if (result == Publication.BACK_PRESSURED) {
            System.out.println("Offer failed due to back pressure");
        } else if (result == Publication.ADMIN_ACTION) {
            System.out.println("Offer failed because of an administration action in the system");
        } else if (result == Publication.NOT_CONNECTED) {
            System.out.println("Offer failed because publisher is not connected to subscriber");
        } else if (result == Publication.CLOSED) {
            System.out.println("Offer failed publication is closed");
        } else if (result == Publication.MAX_POSITION_EXCEEDED) {
            throw new IllegalStateException("Offer failed due to publication reaching max position");
        } else {
            System.out.println("Offer failed due to unknown result code: " + result);
        }
    }

    private static int findLatestRecording(final AeronArchive archive, String channel, int streamId, RecordingDescriptorConsumer consumer) {
        final long fromRecordingId = 0L;
        final int recordCount = 100;

        return archive.listRecordingsForUri(fromRecordingId, recordCount, channel, streamId, consumer);
    }
}

