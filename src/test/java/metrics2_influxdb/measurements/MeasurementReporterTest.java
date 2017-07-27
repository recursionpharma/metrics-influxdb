package metrics2_influxdb.measurements;

import com.yammer.metrics.core.*;
import com.yammer.metrics.core.TimerContext;
import metrics2_influxdb.api.measurements.MetricMeasurementTransformer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

public class MeasurementReporterTest {
	private ListInlinerSender sender;
	private MetricsRegistry registry;
	private MeasurementReporter reporter;

	@BeforeMethod
	public void init() {
		sender = new ListInlinerSender(100);
		registry = new MetricsRegistry();
		reporter = new MeasurementReporter(sender, registry, Clock.defaultClock(), Collections.<String, String>emptyMap(), MetricMeasurementTransformer.NOOP);
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void reportingOneCounterGeneratesOneLine() {
		assertThat(sender.getFrames().size(), is(0));

		// Let's test with one counter
		Counter c = registry.newCounter(
		    new MetricName(MeasurementReporterTest.class, "my-counter"));
		c.inc();
		reporter.run();
		assertThat(sender.getFrames().size(), is(1));
		assertThat(sender.getFrames().get(0), startsWith("my-counter"));
		assertThat(sender.getFrames().get(0), containsString("count=1i"));
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void reportingOneGaugeGeneratesOneLine() {
		assertThat(sender.getFrames().size(), is(0));

		// Let's test with one gauge
    registry.newGauge(
        new MetricName(MeasurementReporterTest.class, "my-gauge"),
        new Gauge<Integer>() {
          @Override
          public Integer value() { return 0; }
        });
		reporter.run();
		assertThat(sender.getFrames().size(), is(1));
		assertThat(sender.getFrames().get(0), startsWith("my-gauge"));
		assertThat(sender.getFrames().get(0), containsString("value=0i"));
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void reportingOneMeterGeneratesOneLine() {
		assertThat(sender.getFrames().size(), is(0));

		// Let's test with one meter
		Meter meter = registry.newMeter(
		    new MetricName(MeasurementReporterTest.class, "my-meter"),
        "event-type",
        TimeUnit.MILLISECONDS);
		meter.mark();
		reporter.run();

		assertThat(sender.getFrames().size(), is(1));
		assertThat(sender.getFrames().get(0), startsWith("my-meter"));

		assertThat(sender.getFrames().get(0), containsString("count=1i"));
		assertThat(sender.getFrames().get(0), containsString("one-minute="));
		assertThat(sender.getFrames().get(0), containsString("five-minute="));
		assertThat(sender.getFrames().get(0), containsString("fifteen-minute="));
		assertThat(sender.getFrames().get(0), containsString("mean-minute="));
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void reportingOneHistogramGeneratesOneLine() {
		assertThat(sender.getFrames().size(), is(0));

		// Let's test with one histogram
		Histogram histogram = registry.newHistogram(
		    new MetricName(MeasurementReporterTest.class, "my-histogram"),
        true);
		histogram.update(0);
		reporter.run();

		assertThat(sender.getFrames().size(), is(1));
		assertThat(sender.getFrames().get(0), startsWith("my-histogram"));

		assertThat(sender.getFrames().get(0), containsString("count=1i"));
		assertThat(sender.getFrames().get(0), containsString("min="));
		assertThat(sender.getFrames().get(0), containsString("max="));
		assertThat(sender.getFrames().get(0), containsString("mean="));
		assertThat(sender.getFrames().get(0), containsString("std-dev="));
		assertThat(sender.getFrames().get(0), containsString("50-percentile="));
		assertThat(sender.getFrames().get(0), containsString("75-percentile="));
		assertThat(sender.getFrames().get(0), containsString("95-percentile="));
		assertThat(sender.getFrames().get(0), containsString("99-percentile="));
		assertThat(sender.getFrames().get(0), containsString("999-percentile="));
		assertThat(sender.getFrames().get(0), containsString("run-count="));
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void reportingOneTimerGeneratesOneLine() {
		assertThat(sender.getFrames().size(), is(0));

		// Let's test with one timer
		Timer meter = registry.newTimer(
		    new MetricName(MeasurementReporterTest.class, "my-timer"),
        TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
		TimerContext ctx = meter.time();

		try {
			Thread.sleep(20);
		} catch (InterruptedException ignored) {
		}

		ctx.stop();

		reporter.run();


		assertThat(sender.getFrames().size(), is(1));
		assertThat(sender.getFrames().get(0), startsWith("my-timer"));

		assertThat(sender.getFrames().get(0), containsString("count=1i"));
		assertThat(sender.getFrames().get(0), containsString("one-minute="));
		assertThat(sender.getFrames().get(0), containsString("five-minute="));
		assertThat(sender.getFrames().get(0), containsString("fifteen-minute="));
		assertThat(sender.getFrames().get(0), containsString("mean-minute="));
		assertThat(sender.getFrames().get(0), containsString("min="));
		assertThat(sender.getFrames().get(0), containsString("max="));
		assertThat(sender.getFrames().get(0), containsString("mean="));
		assertThat(sender.getFrames().get(0), containsString("std-dev="));
		assertThat(sender.getFrames().get(0), containsString("50-percentile="));
		assertThat(sender.getFrames().get(0), containsString("75-percentile="));
		assertThat(sender.getFrames().get(0), containsString("95-percentile="));
		assertThat(sender.getFrames().get(0), containsString("99-percentile="));
		assertThat(sender.getFrames().get(0), containsString("999-percentile="));
		assertThat(sender.getFrames().get(0), containsString("run-count="));
	}
}
