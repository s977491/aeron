package io.aeron.samples.archive;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;

public class RecordingInfoFinder {
    private long controlSessionId;
    private long correlationId;
    private long recordingId;
    private long startTimestamp;
    private long stopTimestamp;
    private long startPosition;
    private long stopPosition;
    private int initialTermId;
    private int segmentFileLength;
    private int termBufferLength;
    private int mtuLength;
    private int sessionId;
    private int streamId;
    private String strippedChannel;
    private String originalChannel;
    private String sourceIdentity;

    public boolean listRecordingForLatestInfo(AeronArchive aeronArchive, int recordId) {
        startPosition = Aeron.NULL_VALUE;
        return aeronArchive.listRecording(recordId, this::checkAndUpdateRecordInfo) > 0;
    }

    void checkAndUpdateRecordInfo(
            long controlSessionId,
            long correlationId,
            long recordingId,
            long startTimestamp,
            long stopTimestamp,
            long startPosition,
            long stopPosition,
            int initialTermId,
            int segmentFileLength,
            int termBufferLength,
            int mtuLength,
            int sessionId,
            int streamId,
            String strippedChannel,
            String originalChannel,
            String sourceIdentity) {

        if (this.startPosition >= startPosition)
            return;
        updateRecordInfo(controlSessionId,
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
                         sourceIdentity);
    }

    void updateRecordInfo(
            long controlSessionId,
            long correlationId,
            long recordingId,
            long startTimestamp,
            long stopTimestamp,
            long startPosition,
            long stopPosition,
            int initialTermId,
            int segmentFileLength,
            int termBufferLength,
            int mtuLength,
            int sessionId,
            int streamId,
            String strippedChannel,
            String originalChannel,
            String sourceIdentity) {
        this.controlSessionId = controlSessionId;
        this.correlationId = correlationId;
        this.recordingId = recordingId;
        this.startTimestamp = startTimestamp;
        this.stopTimestamp = stopTimestamp;
        this.startPosition = startPosition;
        this.stopPosition = stopPosition;
        this.initialTermId = initialTermId;
        this.segmentFileLength = segmentFileLength;
        this.termBufferLength = termBufferLength;
        this.mtuLength = mtuLength;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.strippedChannel = strippedChannel;
        this.originalChannel = originalChannel;
        this.sourceIdentity = sourceIdentity;
    }

    public long getControlSessionId() {
        return controlSessionId;
    }

    public long getCorrelationId() {
        return correlationId;
    }

    public long getRecordingId() {
        return recordingId;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public long getStopTimestamp() {
        return stopTimestamp;
    }

    public long getStartPosition() {
        return startPosition;
    }

    public long getStopPosition() {
        return stopPosition;
    }

    public int getInitialTermId() {
        return initialTermId;
    }

    public int getSegmentFileLength() {
        return segmentFileLength;
    }

    public int getTermBufferLength() {
        return termBufferLength;
    }

    public int getMtuLength() {
        return mtuLength;
    }

    public int getSessionId() {
        return sessionId;
    }

    public int getStreamId() {
        return streamId;
    }

    public String getStrippedChannel() {
        return strippedChannel;
    }

    public String getOriginalChannel() {
        return originalChannel;
    }

    public String getSourceIdentity() {
        return sourceIdentity;
    }

    @Override
    public String toString() {
        return "RecordingInfoFinder{" +
                "controlSessionId=" + controlSessionId +
                ", correlationId=" + correlationId +
                ", recordingId=" + recordingId +
                ", startTimestamp=" + startTimestamp +
                ", stopTimestamp=" + stopTimestamp +
                ", startPosition=" + startPosition +
                ", stopPosition=" + stopPosition +
                ", initialTermId=" + initialTermId +
                ", segmentFileLength=" + segmentFileLength +
                ", termBufferLength=" + termBufferLength +
                ", mtuLength=" + mtuLength +
                ", sessionId=" + sessionId +
                ", streamId=" + streamId +
                ", strippedChannel='" + strippedChannel + '\'' +
                ", originalChannel='" + originalChannel + '\'' +
                ", sourceIdentity='" + sourceIdentity + '\'' +
                '}';
    }
}
