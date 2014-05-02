/*
 * Copyright 2014 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.aeron.conductor;

import uk.co.real_logic.aeron.Channel;
import uk.co.real_logic.aeron.ConsumerChannel;
import uk.co.real_logic.aeron.ProducerControlFactory;
import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.Service;
import uk.co.real_logic.aeron.util.collections.ChannelMap;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.CompletelyIdentifiedMessageFlyweight;
import uk.co.real_logic.aeron.util.command.ConsumerMessageFlyweight;
import uk.co.real_logic.aeron.util.command.MediaDriverFacade;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogAppender;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.LogReader;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;

import static uk.co.real_logic.aeron.util.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.aeron.util.BufferRotationDescriptor.BUFFER_COUNT;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor.BASE_HEADER_LENGTH;


/**
 * Admin thread to take responses and notifications from mediadriver and act on them. As well as pass commands
 * to the mediadriver.
 */
public final class ClientConductor extends Service implements MediaDriverFacade
{
    // TODO: DI this
    private static final byte[] DEFAULT_HEADER = new byte[BASE_HEADER_LENGTH + SIZE_OF_INT];
    private static final int MAX_FRAME_LENGTH = 1024;

    /**
     * Maximum size of the write buffer
     */
    public static final int WRITE_BUFFER_CAPACITY = 256;

    private static final int SLEEP_PERIOD = 1;
    private final RingBuffer recvBuffer;
    private final RingBuffer commandBuffer;
    private final RingBuffer sendBuffer;

    private final BufferUsageStrategy bufferUsage;
    private final AtomicArray<Channel> producers;
    private final AtomicArray<ConsumerChannel> consumers;

    private final ChannelMap<String, Channel> sendNotifiers;
    private final ConsumerMap recvNotifiers;

    private final ConductorErrorHandler errorHandler;
    private final ProducerControlFactory producerControl;

    // Control protocol Flyweights
    private final ChannelMessageFlyweight channelMessage = new ChannelMessageFlyweight();
    private final ConsumerMessageFlyweight receiverMessage = new ConsumerMessageFlyweight();
    private final CompletelyIdentifiedMessageFlyweight requestTermMessage = new CompletelyIdentifiedMessageFlyweight();
    private final CompletelyIdentifiedMessageFlyweight bufferNotificationMessage =
        new CompletelyIdentifiedMessageFlyweight();

    public ClientConductor(final RingBuffer commandBuffer,
                           final RingBuffer recvBuffer,
                           final RingBuffer sendBuffer,
                           final BufferUsageStrategy bufferUsage,
                           final AtomicArray<Channel> producers,
                           final AtomicArray<ConsumerChannel> consumers,
                           final ConductorErrorHandler errorHandler,
                           final ProducerControlFactory producerControl)
    {
        super(SLEEP_PERIOD);

        this.commandBuffer = commandBuffer;
        this.recvBuffer = recvBuffer;
        this.sendBuffer = sendBuffer;
        this.bufferUsage = bufferUsage;
        this.producers = producers;
        this.consumers = consumers;
        this.errorHandler = errorHandler;
        this.producerControl = producerControl;
        this.sendNotifiers = new ChannelMap<>();
        this.recvNotifiers = new ConsumerMap();

        final AtomicBuffer writeBuffer = new AtomicBuffer(ByteBuffer.allocate(WRITE_BUFFER_CAPACITY));
        channelMessage.wrap(writeBuffer, 0);
        receiverMessage.wrap(writeBuffer, 0);
        requestTermMessage.wrap(writeBuffer, 0);
    }

    public void process()
    {
        handleCommandBuffer();
        handleReceiveBuffer();
        processBufferCleaningScan();
    }

    private void processBufferCleaningScan()
    {
        producers.forEach(Channel::processBufferScan);
        consumers.forEach(ConsumerChannel::processBufferScan);
    }

    private void handleCommandBuffer()
    {
        commandBuffer.read(
            (eventTypeId, buffer, index, length) ->
            {
                switch (eventTypeId)
                {
                    case ADD_CHANNEL:
                    case REMOVE_CHANNEL:
                    {
                        channelMessage.wrap(buffer, index);
                        final String destination = channelMessage.destination();
                        final long channelId = channelMessage.channelId();
                        final long sessionId = channelMessage.sessionId();

                        if (eventTypeId == ADD_CHANNEL)
                        {
                            addSender(destination, channelId, sessionId);
                        }
                        else
                        {
                            removeSender(destination, channelId, sessionId);
                        }
                        sendBuffer.write(eventTypeId, buffer, index, length);

                        return;
                    }

                    case ADD_CONSUMER:
                    case REMOVE_CONSUMER:
                    {
                        receiverMessage.wrap(buffer, index);
                        final long[] channelIds = receiverMessage.channelIds();
                        final String destination = receiverMessage.destination();
                        if (eventTypeId == ADD_CONSUMER)
                        {
                            addReceiver(destination, channelIds);
                        }
                        else
                        {
                            removeReceiver(destination, channelIds);
                        }
                        sendBuffer.write(eventTypeId, buffer, index, length);
                        return;
                    }
                    case REQUEST_CLEANED_TERM:
                        sendBuffer.write(eventTypeId, buffer, index, length);
                        return;
                }
            }
        );
    }

    private void addReceiver(final String destination, final long[] channelIds)
    {
        // Not efficient but only happens once per channel ever
        // and is during setup not a latency critical path
        for (final long channelId : channelIds)
        {
            consumers.forEach(
                    receiver ->
                    {
                        if (receiver.matches(destination, channelId))
                        {
                            recvNotifiers.put(destination, channelId, receiver);
                        }
                    }
            );
        }
    }

    private void removeReceiver(final String destination, final long[] channelIds)
    {
        for (final long channelId : channelIds)
        {
            recvNotifiers.remove(destination, channelId);
        }
        // TOOD: release buffers
    }

    private void addSender(final String destination, final long channelId, final long sessionId)
    {
        // see addReceiver re efficiency
        producers.forEach(
                channel ->
                {
                    if (channel.matches(destination, sessionId, channelId))
                    {
                        sendNotifiers.put(destination, sessionId, channelId, channel);
                    }
                }
        );
    }

    private void removeSender(final String destination, final long channelId, final long sessionId)
    {
        if (sendNotifiers.remove(destination, channelId, sessionId) == null)
        {
            // TODO: log an error
        }

        bufferUsage.releaseSenderBuffers(destination, channelId, sessionId);
    }

    private void handleReceiveBuffer()
    {
        recvBuffer.read(
            (eventTypeId, buffer, index, length) ->
            {
                switch (eventTypeId)
                {
                    case NEW_RECEIVE_BUFFER_NOTIFICATION:
                    case NEW_SEND_BUFFER_NOTIFICATION:
                    {
                        bufferNotificationMessage.wrap(buffer, index);
                        final long sessionId = bufferNotificationMessage.sessionId();
                        final long channelId = bufferNotificationMessage.channelId();
                        final long termId = bufferNotificationMessage.termId();
                        final String destination = bufferNotificationMessage.destination();

                        if (eventTypeId == NEW_SEND_BUFFER_NOTIFICATION)
                        {
                            onNewSenderBufferNotification(sessionId, channelId, termId, destination);
                        }
                        else
                        {
                            onNewReceiverBufferNotification(destination, channelId, sessionId, termId);
                        }

                        return;
                    }

                    case ERROR_RESPONSE:
                        errorHandler.onErrorResponse(buffer, index, length);
                        return;
                }
            });
    }

    private void onNewReceiverBufferNotification(final String destination,
                                                 final long channelId,
                                                 final long sessionId,
                                                 final long termId)
    {
        onNewBufferNotification(sessionId, termId,
                                recvNotifiers.get(destination, channelId),
                                i -> newReader(destination, channelId, sessionId, i),
                                LogReader[]::new,
                                (chan, buffers) -> chan.onBuffersMapped(sessionId, buffers));
    }

    private void onNewSenderBufferNotification(final long sessionId,
                                               final long channelId,
                                               final long termId,
                                               final String destination)
    {
        onNewBufferNotification(sessionId, termId,
                                sendNotifiers.get(destination, sessionId, channelId),
                                i -> newAppender(destination, sessionId, channelId, i),
                                LogAppender[]::new,
                                Channel::onBuffersMapped);
    }

    private interface LogFactory<L>
    {
        public L make(int index) throws IOException;
    }

    private <C extends ChannelNotifiable, L>
    void onNewBufferNotification(final long sessionId,
                                 final long termId,
                                 final C channel,
                                 final LogFactory<L> logFactory,
                                 final IntFunction<L[]> logArray,
                                 final BiConsumer<C, L[]> notifier)
    {
        try
        {
            if (channel == null)
            {
                // The new buffer refers to another client process,
                // We can safely ignore it
                return;
            }

            if (!channel.hasTerm(sessionId))
            {
                // You know that you can map all 3 appenders at this point since its the first term
                final L[] logs = logArray.apply(BUFFER_COUNT);
                for (int i = 0; i < BUFFER_COUNT; i++)
                {
                    logs[i] = logFactory.make(i);
                }

                notifier.accept(channel, logs);
                channel.initialTerm(sessionId, termId);
            }
            else
            {
                // TODO is this an error, or a reasonable case?
            }
        }
        catch (final Exception ex)
        {
            // TODO: establish correct client error handling strategy
            ex.printStackTrace();
        }
    }

    public LogAppender newAppender(final String destination,
                                   final long sessionId,
                                   final long channelId,
                                   final int index) throws IOException
    {
        final AtomicBuffer logBuffer = bufferUsage.newSenderLogBuffer(destination, sessionId, channelId, index);
        final AtomicBuffer stateBuffer = bufferUsage.newSenderStateBuffer(destination, sessionId, channelId, index);

        return new LogAppender(logBuffer, stateBuffer, DEFAULT_HEADER, MAX_FRAME_LENGTH);
    }

    private LogReader newReader(final String destination,
                                final long channelId,
                                final long sessionId,
                                final int index) throws IOException
    {
        final AtomicBuffer logBuffer = bufferUsage.newConsumerLogBuffer(destination, channelId, sessionId, index);
        final AtomicBuffer stateBuffer = bufferUsage.newConsumerStateBuffer(destination, channelId, sessionId, index);

        return new LogReader(logBuffer, stateBuffer);
    }

    /* commands to MediaDriver */

    public void sendAddChannel(final String destination, final long sessionId, final long channelId)
    {

    }

    public void sendRemoveChannel(final String destination, final long sessionId, final long channelId)
    {

    }

    public void sendAddConsumer(final String destination, final long[] channelIdList)
    {

    }

    public void sendRemoveConsumer(final String destination, final long[] channelIdList)
    {

    }

    public void sendRequestTerm(final long sessionId, final long channelId, final long termId)
    {

    }

    /* callbacks from MediaDriver */

    public void onErrorResponse(final int code, final byte[] message)
    {
    }

    public void onError(final int code, final byte[] message)
    {
    }

    public void onNewBufferNotification(final long sessionId,
                                        final long channelId,
                                        final long termId,
                                        final boolean isSender,
                                        final String destination)
    {

    }
}