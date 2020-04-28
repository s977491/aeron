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

public class ReplicateMediaDriver {

    public static void main(String[] args) {
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final MediaDriver.Context ctx = new MediaDriver.Context();

        ctx.terminationHook(barrier::signal);

        try (ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(
                new MediaDriver.Context()
                        .termBufferSparseFile(true)
                        .threadingMode(ThreadingMode.SHARED)
//                        .errorHandler(Tests::onError)
                        .spiesSimulateConnection(true)
                        .dirDeleteOnStart(true),
                new Archive.Context()
//                        .maxCatalogEntries(MAX_CATALOG_ENTRIES)
                        .controlChannel(SampleConfiguration.CONTROL_REQUEST_CHANNEL)
                        .archiveClientContext(new AeronArchive.Context().controlResponseChannel(SampleConfiguration.CONTROL_RESPONSE_CHANNEL))
                        .recordingEventsEnabled(false)
                        .replicationChannel(SampleConfiguration.REPLICATION_CHANNEL)
                        .deleteArchiveOnStart(true)
                        .archiveDir(new File(SystemUtil.tmpDirName(), "src-archive"))
                        .fileSyncLevel(0)
                        .threadingMode(ArchiveThreadingMode.DEDICATED)))
        {
            barrier.await();

            System.out.println("Shutdown Archive...");
        }
    }
}
