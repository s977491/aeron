package io.aeron.samples.archive;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.samples.SampleConfiguration;
import org.agrona.SystemUtil;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.File;

import static io.aeron.samples.SampleConfiguration.dstAeronDirectoryName;

public class MediaDriverNormal {

    public static void main(String[] args) {
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final MediaDriver.Context ctx = new MediaDriver.Context();

        ctx.terminationHook(barrier::signal);
        try (ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(
                new MediaDriver.Context()
//                        .aeronDirectoryName(dstAeronDirectoryName)
//                        .publicationTermBufferLength(1 << 29)
//                        .ipcPublicationTermWindowLength(1 << 29)
.ipcTermBufferLength(1 << 16)
//                        .ipcTermBufferLength(1)
                        .termBufferSparseFile(true)
                        .threadingMode(ThreadingMode.SHARED)
//                        .errorHandler(Tests::onError)
                        .spiesSimulateConnection(true)
                        .dirDeleteOnStart(true),
                new Archive.Context()
//                        .aeronDirectoryName(dstAeronDirectoryName)
//                        .controlChannel(SampleConfiguration.DST_CONTROL_REQUEST_CHANNEL)
//                        .archiveClientContext(new AeronArchive.Context().controlResponseChannel(SampleConfiguration.DST_CONTROL_RESPONSE_CHANNEL))
//                        .recordingEventsEnabled(false)
//                        .replicationChannel(SampleConfiguration.DST_REPLICATION_CHANNEL)
                        .deleteArchiveOnStart(true)
                        .archiveDir(new File(SystemUtil.tmpDirName(), "normal-archive"))
                        .fileSyncLevel(0)
                        .segmentFileLength(1 << 17)
                        .threadingMode(ArchiveThreadingMode.DEDICATED))) {
            System.out.println("Started");
            barrier.await();

            System.out.println("Shutdown Archive...");
        }
    }
}
