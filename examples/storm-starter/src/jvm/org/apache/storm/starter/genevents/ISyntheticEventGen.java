package org.apache.storm.starter.genevents;

import java.util.List;

public interface ISyntheticEventGen {
	public void receive(List<String> event);  //event
}
