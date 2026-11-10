package com.pulse.metric.exception;

/** Пользователь не зарегистрирован (404). */
public class UserNotFoundException extends RuntimeException {

    public UserNotFoundException(String userId) {
        super("Пользователь не зарегистрирован: " + userId);
    }
}
