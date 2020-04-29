package io.aeron.samples.archive;

import io.aeron.Subscription;
import io.aeron.archive.client.*;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.status.RecordingPos;
import org.agrona.collections.MutableLong;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.status.CountersReader;

public class RecordingSignalMonitor {

    final MutableLong dstRecordingId = new MutableLong();
    final MutableReference<RecordingSignal> signalRef = new MutableReference<>();

    public void waitForSignal(AeronArchive archive, int timeoutMilli){
        final RecordingSignalAdapter adapter = newRecordingSignalAdapter(signalRef, dstRecordingId, archive);
        signalRef.set(null);
        long startTime = System.currentTimeMillis();
        do
        {
            while (0 == adapter.poll() && System.currentTimeMillis() - startTime < timeoutMilli)
            {
                Thread.yield();
            }
        }   while (signalRef.get() == null && System.currentTimeMillis() - startTime < timeoutMilli);
    }

    public RecordingSignal getSignal() {
        if (signalRef != null) {
            return signalRef.get();
        }
        return null;
    }

    public int getCounterIdByRecordId(AeronArchive archive, int recordId ) {
        final CountersReader counters = archive.context().aeron().countersReader();
        int counterId = RecordingPos.findCounterIdByRecording(counters, recordId);
        if (counterId == CountersReader.NULL_COUNTER_ID) {
            System.out.println("null counter id returned...");
        } else {
            System.out.println("found counter Id");
        }
        return counterId;
    }
    private static RecordingSignalAdapter newRecordingSignalAdapter(
            MutableReference<RecordingSignal> signalRef,
            MutableLong recordingIdRef,
            AeronArchive dstAeronArchive) {
        final ControlEventListener listener =
                (controlSessionId, correlationId, relevantId, code, errorMessage) ->
                {
                    if (code == ControlResponseCode.ERROR)
                    {
                        throw new ArchiveException(errorMessage, (int)relevantId, correlationId);
                    }
                };

        return newRecordingSignalAdapter(listener, signalRef, recordingIdRef, dstAeronArchive);

    }
    private static RecordingSignalAdapter newRecordingSignalAdapter(
            final ControlEventListener listener,
            final MutableReference<RecordingSignal> signalRef,
            final MutableLong recordingIdRef,
            AeronArchive dstAeronArchive)
    {
        final RecordingSignalConsumer consumer =
                (controlSessionId, correlationId, recordingId, subscriptionId, position, transitionType) ->
                {
                    recordingIdRef.set(recordingId);
                    signalRef.set(transitionType);
                };

        final Subscription subscription = dstAeronArchive.controlResponsePoller().subscription();
        final long controlSessionId = dstAeronArchive.controlSessionId();

        return new RecordingSignalAdapter(controlSessionId, listener, consumer, subscription, 10);
    }
}
