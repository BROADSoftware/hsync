package com.kappaware.hsync.ttools;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

public class UnitAppender extends AppenderSkeleton {
	private List<LoggingEvent> events = new ArrayList<LoggingEvent>();

	public void close() {
	}

	public boolean requiresLayout() {
		return false;
	}

	@Override
	protected void append(LoggingEvent event) {
		events.add(event);
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (LoggingEvent event : this.events) {
			sb.append(String.format("%s: %s\n", event.getLevel().toString(), event.getMessage().toString()));
		}
		return sb.toString();
	}
	
	List<LoggingEvent> getEvents() {
		return this.events;
	}
	
	public List<LoggingEvent> searchPattern(String pattern) {
		List<LoggingEvent> l = new Vector<LoggingEvent>();
		for(LoggingEvent event : this.events) {
			if(event.getMessage().toString().contains(pattern)) {
				l.add(event);
			}
		}
		return l;
	}
	
}
