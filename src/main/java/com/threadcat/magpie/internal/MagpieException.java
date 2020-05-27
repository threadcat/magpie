package com.threadcat.magpie.internal;

/**
 * @author threadcat
 */
public class MagpieException extends RuntimeException {

    public MagpieException(String message) {
        super(message);
    }

    public MagpieException(String message, Throwable cause) {
        super(message, cause);
    }
}
