package com.microstrategy.se.kafka.pushapi;

public class MicroStrategyException extends Exception {

	private static final long serialVersionUID = 1L;

	public MicroStrategyException(String message) {
		super(message);
	}

	public MicroStrategyException(Exception e) {
		super(e);
	}

}
