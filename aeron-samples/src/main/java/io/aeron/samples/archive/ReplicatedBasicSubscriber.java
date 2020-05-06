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
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.samples.SampleConfiguration;
import io.aeron.samples.SamplesUtil;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.SigInt;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.samples.SampleConfiguration.srcAeronDirectoryName;

/**
 * A basic subscriber application which requests a replay from the archive and consumes it.
 */
public class ReplicatedBasicSubscriber
{
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;

    // Use a different stream id to avoid clash with live stream
    private static final int REPLAY_STREAM_ID = SampleConfiguration.STREAM_ID + 1;

    private static final String CHANNEL = SampleConfiguration.CHANNEL;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

    public static void main(final String[] args)
    {
        System.out.println("Subscribing to " + CHANNEL + " on stream id " + STREAM_ID);

        final FragmentHandler fragmentHandler = printStringMessage(STREAM_ID);
        final AtomicBoolean running = new AtomicBoolean(true);

        SigInt.register(() -> running.set(false));

        Aeron aeron = Aeron.connect(
                new Aeron.Context().aeronDirectoryName(srcAeronDirectoryName));


        // Create a unique response stream id so not to clash with other archive clients.
        final AeronArchive.Context archiveCtx = new AeronArchive.Context()
                .idleStrategy(YieldingIdleStrategy.INSTANCE)
                .controlRequestChannel(SampleConfiguration.SRC_CONTROL_REQUEST_CHANNEL)
                .controlResponseChannel(SampleConfiguration.SRC_CONTROL_RESPONSE_CHANNEL)
                .aeron(aeron);

        try (AeronArchive archive = AeronArchive.connect(archiveCtx))
        {
            RecordingInfoFinder recordingInfoFinder = new RecordingInfoFinder();
            final long recordingId = findLatestRecording(archive);
            recordingInfoFinder.listRecordingForLatestInfo(archive, 0);
            System.out.println(recordingInfoFinder.toString());
            final long position = 0L;
            final long length = Long.MAX_VALUE;


            try (Subscription subscription = archive.replay(recordingId,
                                                            1177792 - 65,
                                                            65 ,
                                                            CHANNEL,
                                                            REPLAY_STREAM_ID)) {
                while (subscription.imageCount() == 0) {
                    subscription.poll(fragmentHandler, 10);
                    Thread.yield();
                }
                long currentPosition = subscription.imageAtIndex(0).position();
                System.out.println("currentPos" + currentPosition);
                while (currentPosition < 1177792) {
                    subscription.poll(fragmentHandler, 10);
                    long newPosition = subscription.imageAtIndex(0).position();
                    if (newPosition != currentPosition) {
                        currentPosition = newPosition;
                    } else {
                        Thread.yield();
                    }
                }
                ;
            }

//
//
//
//            final long sessionId = archive.startReplay(recordingId, position, length, CHANNEL, REPLAY_STREAM_ID);
//            final String channel = ChannelUri.addSessionId(CHANNEL, (int)sessionId);
//
//            try (Subscription subscription = archive.context().aeron().addSubscription(channel, REPLAY_STREAM_ID))
//            {
//                SamplesUtil.subscriberLoop(fragmentHandler, FRAGMENT_COUNT_LIMIT, running).accept(subscription);
//                System.out.println("Shutting down...");
//            }
        }
    }//1177792

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
}
