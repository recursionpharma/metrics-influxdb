package metrics_influxdb.measurements;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.core.*;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;

import metrics_influxdb.api.measurements.MetricMeasurementTransformer;

public class MeasurementReporter
        extends AbstractPollingReporter
        implements MetricProcessor<Long> {
	private final Sender sender;
	private final Clock clock;
	private Map<String, String> baseTags;
	private MetricMeasurementTransformer transformer;

	public MeasurementReporter(Sender sender, MetricsRegistry registry, Clock clock, Map<String, String> baseTags, MetricMeasurementTransformer transformer) {
		super(registry, "measurement-reporter");
		this.baseTags = baseTags;
		this.sender = sender;
		this.clock = clock;
		this.transformer = transformer;
	}

// 	@SuppressWarnings("rawtypes")
//	@Override
//	public void report(SortedMap<String, Gauge> gauges
//			, SortedMap<String, Counter> counters
//			, SortedMap<String, Histogram> histograms
//			, SortedMap<String, Meter> meters
//			, SortedMap<String, Timer> timers) {
//
//		final long timestamp = clock.time();
//
//		for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
//			sender.send(fromGauge(entry.getKey(), entry.getValue(), timestamp));
//		}
//
//		for (Map.Entry<String, Counter> entry : counters.entrySet()) {
//			sender.send(fromCounter(entry.getKey(), entry.getValue(), timestamp));
//		}
//
//		for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
//			sender.send(fromHistogram(entry.getKey(), entry.getValue(), timestamp));
//		}
//
//		for (Map.Entry<String, Meter> entry : meters.entrySet()) {
//			sender.send(fromMeter(entry.getKey(), entry.getValue(), timestamp));
//		}
//
//		for (Map.Entry<String, Timer> entry : timers.entrySet()) {
//			sender.send(fromTimer(entry.getKey(), entry.getValue(), timestamp));
//		}
//
//		sender.flush();
//	}

	@Override
  public void run() {
    final long timestamp = clock.time();
    final Set<Entry<MetricName, Metric>> metrics = getMetricsRegistry().allMetrics().entrySet();
    try {
      for (Entry<MetricName, Metric> entry : metrics) {
        Metric metric = entry.getValue();
        metric.processWith(this, entry.getKey(), timestamp);
      }
    } catch (Throwable e) {
      e.printStackTrace();
    }

    sender.flush();
  }

  @Override
  public void processTimer(MetricName name, Timer timer, Long timestamp) throws IOException {
    sender.send(fromTimer(name.getName(), timer, timestamp));
  }

  private Measure fromTimer(String metricName, Timer t, long timestamp) {
		Snapshot snapshot = t.getSnapshot();

		Map<String, String> tags = new HashMap<String, String>(baseTags);
		tags.putAll(transformer.tags(metricName));

		Measure measure = new Measure(transformer.measurementName(metricName))
				.timestamp(timestamp)
				.addTag(tags)
				.addValue("count", snapshot.size())
				.addValue("min", t.min())
				.addValue("max", t.max())
				.addValue("mean", t.mean())
				.addValue("std-dev", t.stdDev())
				.addValue("50-percentile", snapshot.getMedian())
				.addValue("75-percentile", snapshot.get75thPercentile())
				.addValue("95-percentile", snapshot.get95thPercentile())
				.addValue("99-percentile", snapshot.get99thPercentile())
				.addValue("999-percentile", snapshot.get999thPercentile())
				.addValue("one-minute", t.oneMinuteRate())
				.addValue("five-minute", t.fiveMinuteRate())
				.addValue("fifteen-minute", t.fifteenMinuteRate())
				.addValue("mean-minute", t.meanRate())
				.addValue("run-count", t.count());

		return measure;
	}

	@Override
  public void processMeter(MetricName name, Metered meter, Long timestamp) throws IOException {
	  sender.send(fromMeter(name.getName(), meter, timestamp));
	}

	private Measure fromMeter(String metricName, Metered mt, long timestamp) {
		Map<String, String> tags = new HashMap<String, String>(baseTags);
		tags.putAll(transformer.tags(metricName));

		Measure measure = new Measure(transformer.measurementName(metricName))
				.timestamp(timestamp)
				.addTag(tags)
				.addValue("count", mt.count())
				.addValue("one-minute", mt.oneMinuteRate())
				.addValue("five-minute", mt.fiveMinuteRate())
				.addValue("fifteen-minute", mt.fifteenMinuteRate())
				.addValue("mean-minute", mt.meanRate());
		return measure;
	}

  @Override
  public void processHistogram(MetricName name, Histogram hist, Long timestamp) throws IOException {
    sender.send(fromHistogram(name.getName(), hist, timestamp));
  }

	private Measure fromHistogram(String metricName, Histogram h, long timestamp) {
		Snapshot snapshot = h.getSnapshot();

		Map<String, String> tags = new HashMap<String, String>(baseTags);
		tags.putAll(transformer.tags(metricName));

		Measure measure = new Measure(transformer.measurementName(metricName))
				.timestamp(timestamp)
				.addTag(tags)
				.addValue("count", snapshot.size())
				.addValue("min", h.min())
				.addValue("max", h.max())
				.addValue("mean", h.mean())
				.addValue("std-dev", h.stdDev())
				.addValue("50-percentile", snapshot.getMedian())
				.addValue("75-percentile", snapshot.get75thPercentile())
				.addValue("95-percentile", snapshot.get95thPercentile())
				.addValue("99-percentile", snapshot.get99thPercentile())
				.addValue("999-percentile", snapshot.get999thPercentile())
				.addValue("run-count", h.count());
		return measure;
	}

  @Override
  public void processCounter(MetricName name, Counter counter, Long timestamp) throws IOException {
    sender.send(fromCounter(name.getName(), counter, timestamp));
  }

	private Measure fromCounter(String metricName, Counter c, long timestamp) {
		Map<String, String> tags = new HashMap<String, String>(baseTags);
		tags.putAll(transformer.tags(metricName));

		Measure measure = new Measure(transformer.measurementName(metricName))
				.timestamp(timestamp)
				.addTag(tags)
				.addValue("count", c.count());

		return measure;
	}

  @Override
  public void processGauge(MetricName name, Gauge gauge, Long timestamp) throws IOException {
    sender.send(fromGauge(name.getName(), gauge, timestamp));
  }

	@SuppressWarnings("rawtypes")
	private Measure fromGauge(String metricName, Gauge g, long timestamp) {
		Map<String, String> tags = new HashMap<String, String>(baseTags);
		tags.putAll(transformer.tags(metricName));

		Measure measure = new Measure(transformer.measurementName(metricName))
				.timestamp(timestamp)
				.addTag(tags);
		Object o = g.value();

		if (o == null) {
			// skip null values
			return null;
		}
		if (o instanceof Long || o instanceof Integer) {
			long value = ((Number)o).longValue();
			measure.addValue("value", value);
		} else if (o instanceof Double) {
			Double d = (Double) o;
			if (d.isInfinite() || d.isNaN()) {
				// skip Infinite & NaN
				return null;
			}
			measure.addValue("value", d.doubleValue());
		} else if (o instanceof Float) {
			Float f = (Float) o;
			if (f.isInfinite() || f.isNaN()) {
				// skip Infinite & NaN
				return null;
			}
			measure.addValue("value", f.floatValue());
		} else {
			String value = ""+o;
			measure.addValue("value", value);
		}

		return measure;
	}
}
