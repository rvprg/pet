package com.rvprg.sumi.tests.helpers;

import java.io.IOException;
import java.io.OutputStream;

import com.rvprg.sumi.log.ByteUtils;
import com.rvprg.sumi.log.SnapshotInstallException;
import com.rvprg.sumi.sm.KeyValueStore;
import com.rvprg.sumi.sm.StateMachine;
import com.rvprg.sumi.sm.StreamableSnapshot;
import com.rvprg.sumi.sm.WritableSnapshot;
import com.rvprg.sumi.sm.KeyValueStore.Data;

public class TestStateMachineImpl implements StateMachine {
    private volatile KeyValueStore store = new KeyValueStore();

    @Override
    public void apply(byte[] command) {
        int key = ByteUtils.intFromBytes(command);
        store.put(new Data(command), new Data(command));
    }

    @Override
    public void installSnapshot(StreamableSnapshot snapshot) throws SnapshotInstallException {
        try {
            store = KeyValueStore.read(snapshot.getInputStream());
        } catch (Exception e) {
            throw new SnapshotInstallException();
        }
    }

    @Override
    public WritableSnapshot getWritableSnapshot() {
        final KeyValueStore thisStore = new KeyValueStore(store);
        return new WritableSnapshot() {
            @Override
            public void write(OutputStream stream) throws IOException {
                thisStore.write(stream);
            }
        };
    }

}
