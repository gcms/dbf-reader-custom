package br.gov.go.saude.dbf.reader;

import org.jamel.dbf.exception.DbfException;
import org.jamel.dbf.structure.DbfField;
import org.jamel.dbf.structure.DbfHeader;

import java.io.DataInput;
import java.util.LinkedHashMap;
import java.util.Map;

public class DBFHeader {
    private final DbfHeader header;
    private DBFField[] fields;
    private Map<String, Integer> indexes;

    public DBFHeader(DbfHeader header) {
        this.header = header;
        this.fields = mapFields(header);
        this.indexes = mapIndexes(fields);
    }

    private static Map<String, Integer> mapIndexes(DBFField[] fields) {
        Map<String, Integer> indexes = new LinkedHashMap<>();
        for (int i = 0; i < fields.length; i++)
            indexes.put(fields[i].getName(), i);

        return indexes;
    }

    private static DBFField[] mapFields(DbfHeader header) {
        DBFField[] fields = new DBFField[header.getFieldsCount()];

        int offset = 0;
        for (int i = 0; i < header.getFieldsCount(); i++) {
            DbfField field = header.getField(i);
            fields[i] = new DBFField(field, offset);
            offset += field.getFieldLength();
        }

        return fields;
    }

    public static DbfHeader read(DataInput dataInput) throws DbfException {
        return DbfHeader.read(dataInput);
    }

    public short getHeaderLength() {
        return header.getHeaderLength();
    }

    public int getFieldsCount() {
        return header.getFieldsCount();
    }

    public byte getYear() {
        return header.getYear();
    }

    public byte getMonth() {
        return header.getMonth();
    }

    public byte getDay() {
        return header.getDay();
    }

    public DBFField getField(int i) {
        return fields[i];
    }

    public DBFField getField(String fieldName) {
        return getField(indexes.get(fieldName));
    }

    public int getNumberOfRecords() {
        return header.getNumberOfRecords();
    }

    public short getRecordLength() {
        return header.getRecordLength();
    }

    public int getFieldIndex(String fieldName) {
        return header.getFieldIndex(fieldName);
    }

    public int getDataStart() {
        return this.header.getHeaderLength() - 32 * (this.header.getFieldsCount() + 1) - 1;
    }

}
