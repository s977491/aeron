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
import io.aeron.ChannelUri;
import io.aeron.Subscription;
import io.aeron.archive.client.*;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.samples.SampleConfiguration;
import io.aeron.samples.SamplesUtil;
import org.agrona.collections.MutableLong;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.SigInt;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.samples.SampleConfiguration.SRC_CONTROL_REQUEST_CHANNEL;
import static io.aeron.samples.SampleConfiguration.dstAeronDirectoryName;

/**
 * A basic subscriber application which requests a replay from the archive and consumes it.
 */
public class ReplicatedRemoteSubscriber
{
    private static int initialTermId;
    private static long stopPosition;
    private static int termBufferLength;
    private static int mtuLength;
    private static long recordingId;
    private static int sessionId;

    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;

    // Use a different stream id to avoid clash with live stream
    private static final int REPLAY_STREAM_ID = SampleConfiguration.STREAM_ID;// + 1;

    private static final String CHANNEL = SampleConfiguration.CHANNEL;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

    public static void main(final String[] args)
    {
        System.out.println("Subscribing to " + CHANNEL + " on stream id " + STREAM_ID);

        final FragmentHandler fragmentHandler = printStringMessage(STREAM_ID);
        final AtomicBoolean running = new AtomicBoolean(true);

        SigInt.register(() -> running.set(false));

        Aeron aeron = Aeron.connect(
                new Aeron.Context().aeronDirectoryName(dstAeronDirectoryName));

        // Create a unique response stream id so not to clash with other archive clients.
        final AeronArchive.Context archiveCtx = new AeronArchive.Context()
                .idleStrategy(YieldingIdleStrategy.INSTANCE)
                .controlRequestChannel(SampleConfiguration.DST_CONTROL_REQUEST_CHANNEL)
                .controlResponseChannel(SampleConfiguration.DST_CONTROL_RESPONSE_CHANNEL)
                .aeron(aeron);

        try (AeronArchive archive = AeronArchive.connect(archiveCtx))
        {

            final long recordingId = 0;
            final long position = 0L;
            final long length = Long.MAX_VALUE;
            long replicatedId = archive.replicate(
                    recordingId, NULL_VALUE, AeronArchive.Configuration.CONTROL_STREAM_ID_DEFAULT,
                    SRC_CONTROL_REQUEST_CHANNEL, null);

            try {
                RecordingSignalMonitor recordingSignalMonitor = new RecordingSignalMonitor();

                recordingSignalMonitor.waitForSignal(archive, 1000);
                System.out.println("Signal return: " + recordingSignalMonitor.getSignal());
                recordingSignalMonitor.waitForSignal(archive, 1000);
                System.out.println("Signal return: " + recordingSignalMonitor.getSignal());
                //            //assertEquals(RecordingSignal.REPLICATE, signalRef.get());

                //load recording...



                try (Subscription subscription = archive.context().aeron().addSubscription(
                        SampleConfiguration.DST_REPLICATION_CHANNEL,
                        REPLAY_STREAM_ID)) {
                    SamplesUtil.subscriberLoop(fragmentHandler, FRAGMENT_COUNT_LIMIT, running).accept(subscription);
                    System.out.println("Shutting down...");
                }

            } finally {
                archive.stopReplication(replicatedId);
            }
//            final long sessionId = archive.startReplay(recordingId, 0, AeronArchive.NULL_LENGTH, CHANNEL, REPLAY_STREAM_ID);
//            final String channel = ChannelUri.addSessionId(CHANNEL, (int)sessionId);
//
//            try (Subscription subscription = archive.context().aeron().addSubscription(channel, REPLAY_STREAM_ID))
//            {
//                SamplesUtil.subscriberLoop(fragmentHandler, FRAGMENT_COUNT_LIMIT, running).accept(subscription);
//                System.out.println("Shutting down...");
//            }
        }
    }

    private static FragmentHandler printStringMessage(int streamId) {
            return (buffer, offset, length, header) ->
            {
                final byte[] data = new byte[length];
                buffer.getBytes(offset, data);

                System.out.println(String.format(
                        "Message to stream %d from session %d (%d@%d) <<%s>>",
                        streamId, header.sessionId(), length, offset, new String(data)));
            };
    }

    private static long findLatestRecording(final AeronArchive archive)
    {
        final MutableLong lastRecordingId = new MutableLong();

        final RecordingDescriptorConsumer consumer =
            (controlSessionId,
            correlationId,
            recordingId,
            startTimestamp,
            stopTimestamp,
            startPosition,
            stopPosition,
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity) -> lastRecordingId.set(recordingId);

        final long fromRecordingId = 0L;
        final int recordCount = 100;

        final int foundCount = archive.listRecordingsForUri(fromRecordingId, recordCount, CHANNEL, STREAM_ID, consumer);

        if (foundCount == 0)
        {
            throw new IllegalStateException("no recordings found");
        }

        return lastRecordingId.get();
    }


    private static int updateWithLatestRecording(String CHANNEL, AeronArchive archive) {
        return findLatestRecording(archive, CHANNEL, STREAM_ID,
                                   (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                                    startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                                    mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                                       if (ReplicatedRemoteSubscriber.stopPosition < stopPosition) {
                                           ReplicatedRemoteSubscriber.initialTermId = initialTermId;
                                           ReplicatedRemoteSubscriber.stopPosition = stopPosition;
                                           ReplicatedRemoteSubscriber.termBufferLength = termBufferLength;
                                           ReplicatedRemoteSubscriber.mtuLength = mtuLength;
                                           ReplicatedRemoteSubscriber.recordingId = recordingId;
                                           ReplicatedRemoteSubscriber.sessionId = sessionId;
                                           System.out.println("sessionId:" + sessionId);

                                       }
                                   });
    }
    private static int findLatestRecording(final AeronArchive archive, String channel, int streamId, RecordingDescriptorConsumer consumer) {
        final long fromRecordingId = 0L;
        final int recordCount = 100;

        return archive.listRecordingsForUri(fromRecordingId, recordCount, channel, streamId, consumer);
    }
}
