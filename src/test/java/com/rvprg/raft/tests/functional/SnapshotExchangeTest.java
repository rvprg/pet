package com.rvprg.raft.tests.functional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

import com.rvprg.raft.tests.helpers.NetworkUtils;
import com.rvprg.raft.transport.MemberId;
import com.rvprg.raft.transport.impl.SnapshotReceiver;
import com.rvprg.raft.transport.impl.SnapshotSender;

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

        SnapshotSender sender = new SnapshotSender(memberId, origFile);
        sender.start();
        SnapshotReceiver receiver = new SnapshotReceiver(memberId, destFile);
        receiver.start().get();

        receiver.shutdown();
        sender.shutdown();

        byte[] origMd5 = DigestUtils.md5(new FileInputStream(origFile));
        byte[] destMd5 = DigestUtils.md5(new FileInputStream(destFile));

        assertThat(origMd5, equalTo(destMd5));

        Files.deleteIfExists(origFile.toPath());
        Files.deleteIfExists(destFile.toPath());
    }

}
