package br.gov.go.saude.dbf.reader;

import org.jamel.dbf.exception.DbfException;
import org.jamel.dbf.structure.DbfDataType;
import org.jamel.dbf.structure.DbfField;

import java.io.DataInput;

public class DBFField {
    private final int offset;
    private DbfField field;

    public DBFField(DbfField field, int offset) {
        this.field = field;
        this.offset = offset;
    }

    public static DbfField read(DataInput in, int fieldIndex) throws DbfException {
        return DbfField.read(in, fieldIndex);
    }

    public String getName() {
        return field.getName();
    }

    public DbfDataType getDataType() {
        return field.getDataType();
    }

    public int getFieldLength() {
        return field.getFieldLength();
    }

    public int getDecimalCount() {
        return field.getDecimalCount();
    }

    public int getFieldIndex() {
        return field.getFieldIndex();
    }

    public int getFieldOffset() {
        return offset;
    }

    public String toString() {
        return String.format("%s: %s", getName(), getDataType());
    }
}
