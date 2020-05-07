package br.gov.go.saude.dbf.reader;

import org.jamel.dbf.exception.DbfException;
import org.jamel.dbf.structure.DbfHeader;

import java.io.*;
import java.nio.charset.Charset;

public class DBFReader implements Closeable {
    protected static final byte DATA_ENDED = 0x1A;
    protected static final byte DATA_DELETED = 0x2A;

    private final Charset charset;
    private final DBFHeader header;
    private final DataInput dataInput;
    private final DBFRecord current;

    private long recordIndex = 0;

    public DBFReader(File file) throws IOException {
        this(file, Charset.defaultCharset());
    }

    public DBFReader(InputStream is) throws IOException {
        this(is, Charset.defaultCharset());
    }

    public DBFReader(File file, Charset charset) throws IOException {
        this(new RandomAccessFile(file, "r"), charset);
    }

    public DBFReader(InputStream is, Charset charset) throws IOException {
        this((DataInput) new DataInputStream(new BufferedInputStream(is)), charset);
    }

    public DBFReader(DataInput dataInput, Charset charset) throws IOException {
        this.dataInput = dataInput;
        this.charset = charset;

        this.header = new DBFHeader(DbfHeader.read(dataInput));
        this.current = new DBFRecord(header, charset);

        int dataStart = this.header.getDataStart();
        if (dataStart > 0)
            dataInput.skipBytes(dataStart);
    }

    public DBFHeader getHeader() {
        return header;
    }

    public Charset getCharset() {
        return charset;
    }

    public long getRecordIndex() {
        return recordIndex;
    }

    public void seek(long recordIndex) throws IOException {
        if (!(dataInput instanceof RandomAccessFile)) {
            if (recordIndex < this.recordIndex)
                throw new IllegalStateException("Can't rewind InputStream with no random access");
            while (this.recordIndex < recordIndex) {
                nextRecord(current);
            }
        } else {
            long skip = recordIndex - this.recordIndex;
            ((RandomAccessFile) dataInput).seek(getHeader().getDataStart() + skip * getHeader().getRecordLength());
        }
    }

    public DBFRow nextRow() {
        return nextRecord(current) ? current.toRow() : null;
    }

    public boolean nextRecord(DBFRecord record) {
        return nextRecordData(record.getData());
    }

    public boolean nextRecordData(byte[] data) {
        try {
            int nextByte;
            do {
                nextByte = dataInput.readByte();
                if (nextByte == DATA_ENDED) {
                    return false;
                } else if (nextByte == DATA_DELETED) {
                    dataInput.skipBytes(header.getRecordLength() - 1);
                }
                recordIndex++;
            } while (nextByte == DATA_DELETED);


            dataInput.readFully(data);
            return true;
        } catch (EOFException e) {
            return false;
        } catch (IOException e) {
            throw new DbfException("Cannot read next record form DBFReader file", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (dataInput instanceof Closeable) {
            ((Closeable) dataInput).close();
        }
    }

}
