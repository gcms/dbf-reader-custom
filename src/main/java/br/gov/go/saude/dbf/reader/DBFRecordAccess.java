package br.gov.go.saude.dbf.reader;

import org.jamel.dbf.exception.DbfException;

import java.math.BigDecimal;
import java.util.Date;

import static java.lang.String.format;

public interface DBFRecordAccess {
    DBFHeader getHeader();

    default Object get(String fieldName) {
        try {
            int fieldIndex = getHeader().getFieldIndex(fieldName);
            return get(fieldIndex);
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new DbfException(format("Field \"%s\" does not exist", fieldName), e);
        }
    }

    Object get(int fieldIndex);

    Object get(DBFField field);

    default BigDecimal getBigDecimal(String fieldName) throws DbfException {
        Object value = get(fieldName);
        return value == null ? null : new BigDecimal(value.toString());
    }

    default BigDecimal getBigDecimal(int fieldIndex) {
        Object value = get(fieldIndex);
        return value == null ? null : new BigDecimal(value.toString());
    }

    default BigDecimal getBigDecimal(DBFField field) {
        Object value = get(field);
        return value == null ? null : new BigDecimal(value.toString());
    }

    default Date getDate(String fieldName) throws DbfException {
        return (Date) get(fieldName);
    }

    default Date getDate(int fieldIndex) throws DbfException {
        return (Date) get(fieldIndex);
    }


    default Date getDate(DBFField field) throws DbfException {
        return (Date) get(field);
    }

    default String getString(DBFField field) throws DbfException {
        return getString(field.getFieldIndex());
    }

    default String getString(String fieldName) throws DbfException {
        return (String) get(fieldName);
    }

    default String getString(int fieldIndex) throws DbfException {
        return (String) get(fieldIndex);
    }

    default boolean getBoolean(String fieldName) throws DbfException {
        Boolean value = (Boolean) get(fieldName);
        return value != null && value;
    }

    default boolean getBoolean(int fieldIndex) throws DbfException {
        Boolean value = (Boolean) get(fieldIndex);
        return value != null && value;
    }

    default boolean getBoolean(DBFField field) throws DbfException {
        Boolean value = (Boolean) get(field);
        return value != null && value;
    }

    default int getInt(String fieldName) throws DbfException {
        return getNumber(fieldName).intValue();
    }

    default short getShort(String fieldName) throws DbfException {
        return getNumber(fieldName).shortValue();
    }

    default byte getByte(String fieldName) throws DbfException {
        return getNumber(fieldName).byteValue();
    }


    default long getLong(String fieldName) throws DbfException {
        return getNumber(fieldName).longValue();
    }

    default float getFloat(String fieldName) throws DbfException {
        return getNumber(fieldName).floatValue();
    }

    default double getDouble(String fieldName) throws DbfException {
        return getNumber(fieldName).doubleValue();
    }

    default Object getObject(String fieldName) throws DbfException {
        return get(fieldName);
    }

    default Number getNumber(String fieldName) {
        Number value = (Number) get(fieldName);
        return value == null ? 0L : value;
    }

}
