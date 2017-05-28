package com.rvprg.raft.tests.functional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

import com.google.common.io.Files;
import com.rvprg.raft.sm.SnapshotDescriptor;
import com.rvprg.raft.tests.helpers.NetworkUtils;
import com.rvprg.raft.transport.ChannelPipelineInitializerImpl;
import com.rvprg.raft.transport.MemberId;
import com.rvprg.raft.transport.SnapshotReceiver;
import com.rvprg.raft.transport.SnapshotSender;

public class SnapshotExchangeTest {
    @Test(timeout = 60000)
    public void testSenderReceiver() throws InterruptedException, ExecutionException, IOException {
        File origFile = File.createTempFile(UUID.randomUUID().toString(), ".tmp");
        File destFile = File.createTempFile(UUID.randomUUID().toString(), ".tmp");

        BufferedOutputStream file = new BufferedOutputStream(new FileOutputStream(origFile));
        Random r = new Random();
        byte[] data = new byte[1024];
        for (int i = 0; i < 1024; ++i) {
            r.nextBytes(data);
            file.write(data);
        }
        file.close();

        MemberId memberId = new MemberId("localhost", NetworkUtils.getRandomFreePort());
        MemberId selfId = new MemberId("localhost", NetworkUtils.getRandomFreePort());

        ChannelPipelineInitializerImpl pipelineInitializer = new ChannelPipelineInitializerImpl();

        CountDownLatch downloadLatch = new CountDownLatch(1);
        SnapshotDescriptor snapshot = new SnapshotDescriptor(Files.createTempDir(), 1, 1);
        // @formatter:off
        SnapshotSender sender = new SnapshotSender(pipelineInitializer, memberId, (e) -> {  });
        sender.setSnapshotDescriptor(snapshot);
        // @formatter:on
        SnapshotReceiver receiver = new SnapshotReceiver(pipelineInitializer, selfId, memberId, "test", destFile, java.nio.file.Files.size(origFile.toPath()),
                (File f, Throwable e) -> {
                    downloadLatch.countDown();
                });
        downloadLatch.await();

        receiver.shutdown();
        sender.shutdown();

        FileInputStream f1 = new FileInputStream(origFile);
        FileInputStream f2 = new FileInputStream(destFile);
        byte[] origMd5 = DigestUtils.md5(f1);
        byte[] destMd5 = DigestUtils.md5(f2);
        f1.close();
        f2.close();

        assertThat(origMd5, equalTo(destMd5));

        java.nio.file.Files.deleteIfExists(origFile.toPath());
        java.nio.file.Files.deleteIfExists(destFile.toPath());
    }

}
