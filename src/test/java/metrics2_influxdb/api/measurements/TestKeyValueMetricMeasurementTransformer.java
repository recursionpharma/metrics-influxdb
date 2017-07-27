package metrics2_influxdb.api.measurements;

import com.google.common.base.Joiner;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsMapContaining.hasEntry;

public class TestKeyValueMetricMeasurementTransformer {
	private KeyValueMetricMeasurementTransformer keyValueTransformer = new KeyValueMetricMeasurementTransformer();

	@Test
	public void aSimpleMetricGeneratesEmptyTags() {
		String metricName = "cpu_load";

		Map<String, String> tags = keyValueTransformer.tags(metricName);

		assertThat(tags, notNullValue());
		assertThat(tags.entrySet(), empty());
	}

	@Test
	public void aSimpleMetricGeneratesIsoMeasurementName() {
		String metricName = "cpu_load";

		String measurementName = keyValueTransformer.measurementName(metricName);

		assertThat(measurementName, notNullValue());
		assertThat(measurementName, is(metricName));
	}

	@Test
	public void aMetricWith2SubStringsGeneratesEmptyTags() {
		String metricName = "cores.cpu_load";

		Map<String, String> tags = keyValueTransformer.tags(metricName);

		assertThat(tags, notNullValue());
		assertThat(tags.entrySet(), empty());
	}

	@Test
	public void aMetricWith2SubStringsGeneratesIsoMeasurementName() {
		String metricName = "cores.cpu_load";

		String measurementName = keyValueTransformer.measurementName(metricName);

		assertThat(measurementName, notNullValue());
		assertThat(measurementName, is(metricName));
	}

	@Test
	public void aMetricWithEven2NPlus1SubStringsGeneratesNTags() {
		String metricName = "server.actarus.cpu_load";

		Map<String, String> tags = keyValueTransformer.tags(metricName);

		assertThat(tags, notNullValue());
		assertThat(tags.entrySet().size(), is(1));
		assertThat(tags, hasEntry("server", "actarus"));
	}

	@Test
	public void aMetricWithEven2NPlus1SubStringsGeneratesMeasurementNameWithLastSubString() {
		String metricName = "server.actarus.cpu_load";

		String measurementName = keyValueTransformer.measurementName(metricName);

		assertThat(measurementName, notNullValue());
		assertThat(measurementName, is("cpu_load"));
	}

	@Test
	public void aMetricWithGeneratedEven2NPlus1SubStringsGeneratesNTags() {
		String baseMetricName = "metric";
		List<String> subStrings = new ArrayList<>();

		int n = 10;
		for (int i = 0; i < n; i++) {
			subStrings.add("key" + i);
			subStrings.add("value" + i);
		}
		subStrings.add(baseMetricName);

		String metricName = Joiner.on(".").join(subStrings);

		Map<String, String> tags = keyValueTransformer.tags(metricName);

		assertThat(tags, notNullValue());
		assertThat(tags.entrySet().size(), is(n));
	}

	@Test
	public void aMetricWithGeneratedEven2NSubStringsGeneratesNMinusOneTags() {
		String baseMetricName = "metric.name";
		List<String> subStrings = new ArrayList<>();

		int nMinusOne = 9;
		for (int i = 0; i < nMinusOne; i++) {
			subStrings.add("key" + i);
			subStrings.add("value" + i);
		}
		subStrings.add(baseMetricName);

		String metricName = Joiner.on(".").join(subStrings);

		Map<String, String> tags = keyValueTransformer.tags(metricName);

		assertThat(tags, notNullValue());
		assertThat(tags.entrySet().size(), is(nMinusOne));
	}

	@Test
	public void aMetricWithGeneratedEven2NPlus1SubStringsGeneratesMeasurementNameWithLastSubString() {
		String baseMetricName = "metric";
		List<String> subStrings = new ArrayList<>();

		int n = 10;
		for (int i = 0; i < n; i++) {
			subStrings.add("key" + i);
			subStrings.add("value" + i);
		}
		subStrings.add(baseMetricName);

		String metricName = Joiner.on(".").join(subStrings);

		String measurementName = keyValueTransformer.measurementName(metricName);

		assertThat(measurementName, notNullValue());
		assertThat(measurementName, is(baseMetricName));
	}

	@Test
	public void aMetricWithGeneratedEven2NSubStringsGeneratesMeasurementNameWithLast2SubString() {
		String baseMetricName = "metric.name";
		List<String> subStrings = new ArrayList<>();

		int nMinusOne = 9;
		for (int i = 0; i < nMinusOne; i++) {
			subStrings.add("key" + i);
			subStrings.add("value" + i);
		}
		subStrings.add(baseMetricName);

		String metricName = Joiner.on(".").join(subStrings);

		String measurementName = keyValueTransformer.measurementName(metricName);

		assertThat(measurementName, notNullValue());
		assertThat(measurementName, is(baseMetricName));
	}
}
