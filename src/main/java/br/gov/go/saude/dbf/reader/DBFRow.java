package br.gov.go.saude.dbf.reader;


import java.nio.charset.Charset;

public class DBFRow implements DBFRecordAccess {
    private final DBFHeader header;
    private final Object[] values;
    private final Charset charset;

    public DBFRow(DBFHeader header, DBFRecord record, Charset charset) {
        this.header = header;
        this.values = mapRecord(header, record);
        this.charset = charset;
    }

    private static Object[] mapRecord(DBFHeader header, DBFRecord record) {
        Object[] values = new Object[header.getFieldsCount()];
        for (int i = 0; i < header.getFieldsCount(); i++) {
            values[i] = record.readFieldValue(header.getField(i));
        }
        return values;
    }

    @Override
    public DBFHeader getHeader() {
        return header;
    }

    @Override
    public Object get(DBFField field) {
        return get(field.getFieldIndex());
    }


    @Override
    public Object get(int fieldIndex) {
        return values[fieldIndex];
    }

}
