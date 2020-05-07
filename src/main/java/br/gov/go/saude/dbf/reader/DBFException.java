package br.gov.go.saude.dbf.reader;

public class DBFException extends RuntimeException {
    public DBFException(String message, Exception ex) {
        super(message, ex);
    }
}
