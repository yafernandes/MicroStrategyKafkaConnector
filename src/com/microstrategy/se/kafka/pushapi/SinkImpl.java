package com.microstrategy.se.kafka.pushapi;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SinkImpl extends SinkTask {

	private Collection<Map<String, String>> columns = null;
	final private ObjectMapper mapper = new ObjectMapper();
	static final private HashMap<Class<?>, String> mstrTypes = new HashMap<Class<?>, String>();
	final private Map<Object, Map<String, Object>> buffer = new HashMap<Object, Map<String, Object>>();
	private Map<String, String> props;

	static {
		mstrTypes.put(Long.class, "BIGINTEGER");
	}

	@Override
	public String version() {
		return "0.0.1a";
	}

	@Override
	@SuppressWarnings("unchecked")
	public void put(Collection<SinkRecord> records) {
		for (SinkRecord record : records) {
			buffer.put(record.key(), (Map<String, Object>) record.value());
		}
		if (buffer.size() > 5000) {
			context.requestCommit();
		}
	}

	private Collection<Map<String, String>> getColumns(Map<String, Object> row) {
		Collection<Map<String, String>> columns = new ArrayList<Map<String, String>>();
		for (Entry<String, Object> entry : row.entrySet()) {
			Map<String, String> entryMap = new HashMap<String, String>();
			entryMap.put("name", entry.getKey());
			entryMap.put("dataType", mstrTypes.getOrDefault(entry.getValue().getClass(), "STRING"));
			columns.add(entryMap);
			System.out.println(entry.getValue().getClass() + " => "
					+ mstrTypes.getOrDefault(entry.getValue().getClass(), "STRING"));
		}
		return columns;
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
		String tableName = "topic";
		if (!buffer.isEmpty()) {
			MicroStrategy mstr = new MicroStrategy(props.get(MicroStrategySink.CONFIG_LIBRARY_URL),
					props.get(MicroStrategySink.CONFIG_USER), props.get(MicroStrategySink.CONFIG_PASSWORD));
			try {
				Map<String, Object> tableDefinition = new HashMap<String, Object>();
				tableDefinition.put("name", tableName);
				if (columns == null) {
					columns = getColumns(buffer.values().iterator().next());
				}
				tableDefinition.put("columnHeaders", columns);

				byte[] array = mapper.writeValueAsString(buffer.values()).getBytes(Charset.forName("utf-8"));
				tableDefinition.put("data", new String(Base64.getEncoder().encode(array), Charset.forName("utf-8")));

				mstr.connect();
				mstr.setProject(props.get(MicroStrategySink.CONFIG_PROJECT));
				mstr.setTarget(props.get(MicroStrategySink.CONFIG_CUBE), tableName);
				mstr.push(tableDefinition);
			} catch (JsonProcessingException e) {
				e.printStackTrace();
				System.err.println(buffer.size() + " records failed to load.");
			} catch (MicroStrategyException e) {
				e.printStackTrace();
				System.err.println(buffer.size() + " records failed to load.");
			} finally {
				mstr.logout();
			}
			buffer.clear();
		}
		super.flush(currentOffsets);
	}

	@Override
	public void start(Map<String, String> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

}
