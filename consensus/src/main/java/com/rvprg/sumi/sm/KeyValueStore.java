package com.rvprg.sumi.sm;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Externalizable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

import com.github.andrewoma.dexx.collection.HashMap;
import com.github.andrewoma.dexx.collection.Pair;

public class KeyValueStore implements Externalizable {
    private AtomicReference<HashMap<Data, Data>> map = new AtomicReference<>(HashMap.<Data, Data> empty());

    public KeyValueStore(KeyValueStore s) {
        this.map.set(s.map.get());
    }

    @Override
    public String toString() {
        return "KeyValueStore [map=" + map + "]";
    }

    public static class Data {
        public volatile byte[] data;

        public Data(byte[] data) {
            this.data = data;
        }

        public Data() {
            this.data = new byte[0];
        }

        public byte[] getData() {
            return this.data;
        }
    }

    public void put(Data key, Data value) {
        map.updateAndGet((x) -> x.put(key, value));
    }

    public Data get(Data key) {
        return map.get().get(key);
    }

    public KeyValueStore() {
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        for (int i = 0; i < size; ++i) {
            int length = in.readInt();
            byte[] data = new byte[length];
            in.readFully(data);
            Data key = new Data(data);

            length = in.readInt();
            data = new byte[length];
            in.readFully(data);
            Data value = new Data(data);

            put(key, value);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        HashMap<Data, Data> currMap = map.get();
        Iterator<Pair<Data, Data>> it = currMap.iterator();
        out.writeInt(currMap.size());
        while (it.hasNext()) {
            Pair<Data, Data> e = it.next();
            out.writeInt(e.component1().getData().length);
            out.write(e.component1().getData());

            out.writeInt(e.component2().getData().length);
            out.write(e.component2().getData());
        }
    }

    public static KeyValueStore read(File file) throws IOException, ClassNotFoundException {
        BufferedInputStream inputStream = null;
        ObjectInputStream objectInputStream = null;
        try {
            inputStream = new BufferedInputStream(new FileInputStream(file));
            objectInputStream = new ObjectInputStream(inputStream);
            return (KeyValueStore) objectInputStream.readObject();
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception e) {
                }
            }
            if (objectInputStream != null) {
                try {
                    objectInputStream.close();
                } catch (Exception e) {
                }
            }
        }
    }

    public void write(File file) {
        BufferedOutputStream outputStream = null;
        ObjectOutputStream objectOutputStream = null;
        try {
            outputStream = new BufferedOutputStream(new FileOutputStream(file));
            objectOutputStream = new ObjectOutputStream(outputStream);
            objectOutputStream.writeObject(this);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (Exception e) {
                }
            }
            if (objectOutputStream != null) {
                try {
                    objectOutputStream.close();
                } catch (Exception e) {
                }
            }
        }
    }

    public void write(OutputStream outputStream) throws IOException {
        ObjectOutputStream objectOutputStream = null;
        objectOutputStream = new ObjectOutputStream(outputStream);
        objectOutputStream.writeObject(this);
    }

    public static KeyValueStore read(InputStream inputStream) throws IOException, ClassNotFoundException {
        ObjectInputStream objectInputStream = null;
        objectInputStream = new ObjectInputStream(inputStream);
        return (KeyValueStore) objectInputStream.readObject();
    }
}
