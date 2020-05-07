package br.gov.go.saude.dbf.reader;

import org.jamel.dbf.exception.DbfException;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;

public class DBFRecord implements DBFRecordAccess {
    private final DBFHeader header;
    private final Charset charset;
    private byte[] data;

    public DBFRecord(DBFHeader header) {
        this(header, Charset.defaultCharset());
    }

    public DBFRecord(DBFReader reader) {
        this(reader.getHeader(), reader.getCharset());
    }

    public DBFRecord(DBFHeader header, Charset charset) {
        this.header = header;
        this.charset = charset;
        this.data = new byte[header.getRecordLength() - 1];
    }

    @Override
    public DBFHeader getHeader() {
        return header;
    }

    public DBFRow toRow() {
        return new DBFRow(header, this, charset);
    }

    @Override
    public Object get(int fieldIndex) {
        return get(header.getField(fieldIndex));
    }

    public byte[] getData() {
        return data;
    }

    public void copyTo(DBFRecord record) {
        if (record.getHeader() != this.getHeader())
            throw new DbfException("Unable to copy data from different headers");

        System.arraycopy(this.data, 0, record.getData(), 0, this.data.length);
    }

    public DBFRecord copy() {
        DBFRecord record = new DBFRecord(header, charset);
        copyTo(record);
        return record;
    }

    public Object readFieldValue(DBFField field) {
        switch (field.getDataType()) {
            case CHAR:
                return readString(field, charset);
            case NUMERIC:
                return readNumeric(field);
            case FLOAT:
                return readFloat(field);
            case DATE:
                return readDate(field);
            case LOGICAL:
                return readLogical(field);
            case MEMO:
                return readMemoLink(field);
            default:
                return null;
        }
    }

    public byte[] readFieldData(DBFField field) {
        return readFieldData(field.getFieldOffset(), field.getFieldLength());
    }

    public byte[] readFieldData(int offset, int length) {
        return Arrays.copyOfRange(data, offset, offset + length);
    }


    public String readString(DBFField field, Charset charset) {
        return readString(field.getFieldOffset(), field.getFieldLength(), charset);
    }

    public String readString(int offset, int length) {
        return readString(offset, length, charset);
    }


    public String readString(int offset, int length, Charset charset) {
        int newLength = Utils.calcLengthTrimmingSpaces(data, offset, length);
        return new String(data, offset, newLength, charset);
    }


    public Number readNumeric(DBFField field) {
        try {
            return readNumeric(field.getFieldOffset(), field.getFieldLength());
        } catch (NumberFormatException e) {
            throw new DbfException(String.format("Failed to parse Number from %s (%s)", field.getName(), readString(field, charset)), e);
        }
    }

    public Number readNumeric(int offset, int length) throws NumberFormatException {
        int newLength = Utils.calcLengthTrimmingSpaces(data, offset, length);
        if (newLength == 0 || Utils.contains(data, offset, offset + newLength, (byte) '?'))
            return null;

        String strValue = new String(data, offset, newLength);
        return strValue.isEmpty() ? 0L : Double.parseDouble(strValue);
    }


    public Date readDate(DBFField field) {
        int offset = field.getFieldOffset();

        int year = Utils.parsePositiveInt(data, offset, offset + 4);
        int month = Utils.parsePositiveInt(data, offset + 4, offset + 4 + 2);
        int day = Utils.parsePositiveInt(data, offset + 6, offset + 6 + 2);

        return new GregorianCalendar(year, month - 1, day).getTime();
    }

    public Float readFloat(DBFField field) {
        try {
            return readFloat(field.getFieldOffset(), field.getFieldLength());
        } catch (NumberFormatException e) {
            throw new DbfException("Failed to parse Float from " + field.getName(), e);
        }
    }

    public Float readFloat(int offset, int length) {
        int newLength = Utils.calcLengthTrimmingSpaces(data, offset, length);
        if (newLength == 0 || Utils.contains(data, offset, offset + newLength, (byte) '?'))
            return null;

        return Float.valueOf(new String(data, offset, newLength));
    }

    public Boolean readLogical(DBFField field) {
        return readLogical(field.getFieldOffset());
    }

    public Boolean readLogical(int offset) {
        byte value = data[offset];
        boolean isTrue = (value == 'Y' || value == 'y' || value == 'T' || value == 't');
        return isTrue ? Boolean.TRUE : Boolean.FALSE;
    }

    public Number readMemoLink(DBFField field) {
        switch (field.getFieldLength()) {
            case 4:
                return Utils.readLittleEndian(data, field.getFieldOffset());
            case 10:
                return readNumeric(field);
            default:
                throw new DbfException("Unknown MEMO mode: " + field.getFieldLength());
        }
    }

    @Override
    public Object get(DBFField field) {
        return readFieldValue(field);
    }

}
