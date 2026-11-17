package com.pulse.metric.exception;

/** Email уже зарегистрирован (409). */
public class EmailAlreadyRegisteredException extends RuntimeException {

    public EmailAlreadyRegisteredException() {
        super("Email уже зарегистрирован");
    }
}
