package com.rvprg.raft.sm;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Optional;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Hex;

import com.rvprg.raft.log.ByteUtils;

public class SnapshotDescriptor implements ReadableSnapshot {
    private final File fileName;
    private final String snapshotId;
    private final Random random = new Random();
    private final long index;
    private final int term;
    private final String uniqueId;

    public static int compare(SnapshotDescriptor s1, SnapshotDescriptor s2) {
        if (s1.term == s2.term) {
            return Long.compare(s1.index, s2.index);
        }
        return Integer.compare(s1.term, s2.term);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (index ^ (index >>> 32));
        result = prime * result + term;
        result = prime * result + ((uniqueId == null) ? 0 : uniqueId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SnapshotDescriptor other = (SnapshotDescriptor) obj;
        if (index != other.index)
            return false;
        if (term != other.term)
            return false;
        if (uniqueId == null) {
            if (other.uniqueId != null)
                return false;
        } else if (!uniqueId.equals(other.uniqueId))
            return false;
        return true;
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public long getIndex() {
        return index;
    }

    public int getTerm() {
        return term;
    }

    @Override
    public String toString() {
        return "SnapshotDescriptor [fileName=" + fileName + "]";
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public OutputStream getOutputStream() throws FileNotFoundException {
        return new BufferedOutputStream(new FileOutputStream(fileName, false));
    }

    public InputStream getInputStream() throws FileNotFoundException {
        return new BufferedInputStream(new FileInputStream(fileName));
    }

    private final static Pattern snapshotFileNamePattern = Pattern.compile("snapshot-([A-Fa-f0-9]+)-([A-Fa-f0-9]+)-([A-Fa-f0-9]+)");

    public SnapshotDescriptor(String snapshotFile) {
        this(new File(snapshotFile));
    }

    public static SnapshotDescriptor getLatestSnapshotDescriptor(File folder) {
        File[] snapshotFiles = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String fileName) {
                return snapshotFileNamePattern.matcher(fileName).find();
            }
        });
        Optional<SnapshotDescriptor> latest = Arrays.stream(snapshotFiles).map(x -> new SnapshotDescriptor(x)).max(SnapshotDescriptor::compare);
        if (latest.isPresent()) {
            return latest.get();
        }
        return null;
    }

    public SnapshotDescriptor(File snapshotFile) {
        Matcher matcher = snapshotFileNamePattern.matcher(snapshotFile.getName());
        if (matcher.find()) {
            try {
                this.uniqueId = matcher.group(1);
                String indexHex = matcher.group(2);
                String termHex = matcher.group(3);
                byte[] indexArr = Hex.decodeHex(indexHex.toCharArray());
                byte[] termArr = Hex.decodeHex(termHex.toCharArray());
                this.index = ByteUtils.longFromBytes(indexArr);
                this.term = ByteUtils.intFromBytes(termArr);
                this.fileName = snapshotFile;
                this.snapshotId = snapshotFile.getName();
            } catch (Exception e) {
                throw new IllegalArgumentException("Malformed snapshot filename");
            }
        } else {
            throw new IllegalArgumentException("Malformed snapshot filename");
        }
    }

    public SnapshotDescriptor(File folder, long index, int term) {
        this.index = index;
        this.term = term;
        byte[] prefix = new byte[4];
        random.nextBytes(prefix);
        this.uniqueId = Hex.encodeHexString(prefix);
        String snapshotId = "snapshot-" + uniqueId + "-" +
                Hex.encodeHexString(ByteUtils.longToBytes(index)) + "-" +
                Hex.encodeHexString(ByteUtils.intToBytes(term));

        this.fileName = new File(folder, snapshotId);
        this.snapshotId = snapshotId;
    }

    public File getFileName() {
        return this.fileName;
    }

    public long getSize() {
        return this.fileName.length();
    }

    @Override
    public InputStream read() throws FileNotFoundException {
        return getInputStream();
    }
}
