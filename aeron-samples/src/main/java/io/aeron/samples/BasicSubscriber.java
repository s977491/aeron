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
package io.aeron.samples;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.HdrHistogram.Histogram;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.SigInt;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is a Basic Aeron subscriber application.
 * <p>
 * The application subscribes to a default channel and stream ID.  These defaults can
 * be overwritten by changing their value in {@link SampleConfiguration} or by
 * setting their corresponding Java system properties at the command line, e.g.:
 * -Daeron.sample.channel=aeron:udp?endpoint=localhost:5555 -Daeron.sample.streamId=20
 * This application only handles non-fragmented data. A DataHandler method is called
 * for every received message or message fragment.
 * For an example that implements reassembly of large, fragmented messages, see
 * {@link MultipleSubscribersWithFragmentAssembly}.
 */
public class BasicSubscriber
{
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final String CHANNEL = SampleConfiguration.CHANNEL;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final boolean EMBEDDED_MEDIA_DRIVER = SampleConfiguration.EMBEDDED_MEDIA_DRIVER;
    private static final Histogram HISTOGRAM = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);

    public static void main(final String[] args)
    {
        System.out.println("Subscribing 2 to " + CHANNEL + " on stream id " + STREAM_ID);

        final MediaDriver driver = EMBEDDED_MEDIA_DRIVER ? MediaDriver.launchEmbedded() : null;
        final Aeron.Context ctx = new Aeron.Context()
            .availableImageHandler(SamplesUtil::printAvailableImage)
            .unavailableImageHandler(BasicSubscriber::unavail);

        if (EMBEDDED_MEDIA_DRIVER)
        {
            ctx.aeronDirectoryName(driver.aeronDirectoryName());
        }

        final FragmentHandler fragmentHandler = BasicSubscriber::handler;
        final AtomicBoolean running = new AtomicBoolean(true);

        // Register a SIGINT handler for graceful shutdown.
        SigInt.register(() -> running.set(false));

        // Create an Aeron instance using the configured Context and create a
        // Subscription on that instance that subscribes to the configured
        // channel and stream ID.
        // The Aeron and Subscription classes implement "AutoCloseable" and will automatically
        // clean up resources when this try block is finished
        HISTOGRAM.reset();
        try (Aeron aeron = Aeron.connect(ctx);
            Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID))
        {
            SamplesUtil.subscriberLoop(fragmentHandler, FRAGMENT_COUNT_LIMIT, running).accept(subscription);

            System.out.println("Shutting down...");
        }

        CloseHelper.quietClose(driver);
    }

    private static void unavail(Image image) {
        SamplesUtil.printUnavailableImage(image);

        HISTOGRAM.outputPercentileDistribution(System.out, 1000.0);

    }

    private static void handler(DirectBuffer directBuffer, int i, int len, Header header) {
        SamplesUtil.test(1);
//        SamplesUtil.printStringMessage(STREAM_ID);
        long start = directBuffer.getLong(i);
        long end = System.nanoTime();
        if (start > end ) {
            System.out.println("Start:" + start + ", end" + end);
        }
        else {
            HISTOGRAM.recordValue(end - start);
        }
    }
}
