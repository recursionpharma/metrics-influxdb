package metrics2_influxdb.measurements;

import com.yammer.metrics.core.*;
import metrics2_influxdb.api.measurements.MetricMeasurementTransformer;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

public class MeasurementReporterWithBaseTagsTest {
	private ListInlinerSender sender = new ListInlinerSender(100);
	private MetricsRegistry registry = new MetricsRegistry();

	@SuppressWarnings("rawtypes")
	@Test
	public void generatedMeasurementContainsBaseTags() {
		String serverKey = "server";
		String serverName = "icare";
		Map<String, String> baseTags = new HashMap<>();
		baseTags.put(serverKey, serverName);

		MeasurementReporter reporter = new MeasurementReporter(sender, registry, Clock.defaultClock(), baseTags, MetricMeasurementTransformer.NOOP);
		assertThat(sender.getFrames().size(), is(0));

		// Let's test with one counter
		Counter c = registry.newCounter(new MetricName(MeasurementReporterWithBaseTagsTest.class, "my-counter"));
		c.inc();
		reporter.run();
		assertThat(sender.getFrames().size(), is(1));
		assertThat(sender.getFrames().get(0), startsWith("my-counter"));
		assertThat(sender.getFrames().get(0), containsString("count=1i"));
		assertThat(sender.getFrames().get(0), containsString("server=icare"));
		assertThat(sender.getFrames().get(0), startsWith(String.format("%s,%s=%s", "my-counter", serverKey, serverName)));

		reporter.shutdown();
	}
}
