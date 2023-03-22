package com.demo.model;

public class RepositoryException extends RuntimeException {
    protected int status;

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    protected RepositoryException() {
    }

    public RepositoryException(String message) {
        super(message);
    }

    public RepositoryException(int code, String message) {
        super(message);
        this.status = code;
    }

    public RepositoryException(String message, Throwable cause) {
        super(message, cause);
    }

    public RepositoryException(int code, String message, Throwable cause) {
        super(message, cause);
        this.status = code;
    }
}
