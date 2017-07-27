//	metrics-influxdb
//
//	Written in 2014 by David Bernard <dbernard@novaquark.com>
//
//	[other author/contributor lines as appropriate]
//
//	To the extent possible under law, the author(s) have dedicated all copyright and
//	related and neighboring rights to this software to the public domain worldwide.
//	This software is distributed without any warranty.
//
//	You should have received a copy of the CC0 Public Domain Dedication along with
//	this software. If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
package metrics_influxdb.v08;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.core.*;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reporter which publishes metric values to a InfluxDB server.
 *
 * @see <a href="http://influxdb.org/">InfluxDB - An open-source distributed
 *      time series database with no external dependencies.</a>
 */
public class ReporterV08 extends AbstractPollingReporter
                         implements MetricProcessor<Long> {
	private static String[] COLUMNS_TIMER = {
			"time", "count"
			, "min", "max", "mean", "std-dev"
			, "50-percentile", "75-percentile", "95-percentile", "99-percentile", "999-percentile"
			, "one-minute", "five-minute", "fifteen-minute", "mean-rate"
			, "run-count"
	};
	private static String[] COLUMNS_HISTOGRAM = {
			"time", "count"
			, "min", "max", "mean", "std-dev"
			, "50-percentile", "75-percentile", "95-percentile", "99-percentile", "999-percentile"
			, "run-count"
	};
	private static String[] COLUMNS_COUNT = {
			"time", "count"
	};
	private static String[] COLUMNS_GAUGE = {
			"time", "value"
	};
	private static String[] COLUMNS_METER = {
			"time", "count"
			, "one-minute", "five-minute", "fifteen-minute", "mean-rate"
	};

	static final Logger LOGGER = LoggerFactory.getLogger(ReporterV08.class);

	private final Influxdb influxdb;
	private final Clock clock;
	private final String prefix;
	// Optimization : use pointsXxx to reduce object creation, by reuse as arg of
	// Influxdb.appendSeries(...)
	private final Object[][] pointsTimer = { {
		0l,
		0,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0l
	} };
	private final Object[][] pointsHistogram = { {
		0l,
		0,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0l
	} };
	private final Object[][] pointsCounter = { {
		0l,
		0l
	} };
	private final Object[][] pointsGauge = { {
		0l,
		null
	} };
	private final Object[][] pointsMeter = { {
		0l,
		0,
		0.0d,
		0.0d,
		0.0d,
		0.0d
	} };

	protected final boolean skipIdleMetrics;
	protected final Map<String, Long> previousValues;

	public ReporterV08(MetricsRegistry registry,
			Influxdb influxdb,
			Clock clock,
			String prefix,
			boolean skipIdleMetrics) {
		super(registry, "influxdb-reporter");
		this.skipIdleMetrics = skipIdleMetrics;
		this.previousValues = new TreeMap<String, Long>();
		this.influxdb = influxdb;
		this.clock = clock;
		this.prefix = (prefix == null) ? "" : (prefix.trim() + ".");
	}

	/**
	 * Returns true if this metric is idle and should be skipped.
	 *
	 * @param name
	 * @param count
	 * @return true if the metric should be skipped
	 */
	protected boolean canSkipMetric(String name, long count) {
		boolean isIdle = calculateDelta(name, count) == 0L;
		if (skipIdleMetrics && !isIdle) {
			previousValues.put(name, count);
		}
		return skipIdleMetrics && isIdle;
	}

	/**
	 * Calculate the delta from the current value to the previous reported value.
	 */
	private long calculateDelta(String name, long count) {
		Long previous = previousValues.get(name);
		if (previous == null) {
			// unknown metric, force non-zero delta to report
			return -1L;
		}
		if (count < previous) {
			LOGGER.warn("Saw a non-monotonically increasing value for metric '{}'", name);
			return 0L;
		}
		return count - previous;
	}

//	@Override
//	@SuppressWarnings("rawtypes")
//	public void report(SortedMap<String, Gauge> gauges,
//			SortedMap<String, Counter> counters,
//			SortedMap<String, Histogram> histograms,
//			SortedMap<String, Meter> meters,
//			SortedMap<String, Timer> timers) {
//		final long timestamp = clock.time();
//
//		// oh it'd be lovely to use Java 7 here
//		try {
//			influxdb.resetRequest();
//
//			for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
//				reportGauge(entry.getKey(), entry.getValue(), timestamp);
//			}
//
//			for (Map.Entry<String, Counter> entry : counters.entrySet()) {
//				reportCounter(entry.getKey(), entry.getValue(), timestamp);
//			}
//
//			for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
//				reportHistogram(entry.getKey(), entry.getValue(), timestamp);
//			}
//
//			for (Map.Entry<String, Meter> entry : meters.entrySet()) {
//				reportMeter(entry.getKey(), entry.getValue(), timestamp);
//			}
//
//			for (Map.Entry<String, Timer> entry : timers.entrySet()) {
//				reportTimer(entry.getKey(), entry.getValue(), timestamp);
//			}
//
//			if (influxdb.hasSeriesData()) {
//				influxdb.sendRequest(true, false);
//			}
//		} catch (Exception e) {
//			LOGGER.warn("Unable to report to InfluxDB. Discarding data.", e);
//		}
//	}

	@Override
  public void run() {
    final long timestamp = clock.time();
    final Set<Entry<MetricName, Metric>> metrics = getMetricsRegistry().allMetrics().entrySet();
    try {
      for (Entry<MetricName, Metric> entry: metrics) {
          Metric metric = entry.getValue();
          metric.processWith(this, entry.getKey(), timestamp);
      }
      if (influxdb.hasSeriesData()) {
        influxdb.sendRequest(true, false);
      }
    } catch (Throwable e) {
      LOGGER.warn("Unable to report to InfluxDB. Discarding data.", e);
    }
  }

  @Override
  public void processTimer(MetricName name, Timer timer, Long timestamp) throws IOException {
    reportTimer(name.getName(), timer, timestamp);
  }

	private void reportTimer(String name, Timer timer, long timestamp) {
		if (canSkipMetric(name, timer.count())) {
			return;
		}
		final Snapshot snapshot = timer.getSnapshot();
		Object[] p = pointsTimer[0];
		p[0] = influxdb.convertTimestamp(timestamp);
		p[1] = snapshot.size();
		p[2] = timer.min();
		p[3] = timer.max();
		p[4] = timer.mean();
		p[5] = timer.stdDev();
		p[6] = snapshot.getMedian();
		p[7] = snapshot.get75thPercentile();
		p[8] = snapshot.get95thPercentile();
		p[9] = snapshot.get99thPercentile();
		p[10] = snapshot.get999thPercentile();
		p[11] = timer.oneMinuteRate();
		p[12] = timer.fiveMinuteRate();
		p[13] = timer.fifteenMinuteRate();
		p[14] = timer.meanRate();
		p[15] = timer.count();
		assert (p.length == COLUMNS_TIMER.length);
		influxdb.appendSeries(prefix, name, ".timer", COLUMNS_TIMER, pointsTimer);
	}

  @Override
  public void processHistogram(MetricName name, Histogram histogram, Long timestamp) throws IOException {
    reportHistogram(name.getName(), histogram, timestamp);
  }

	private void reportHistogram(String name, Histogram histogram, long timestamp) {
		if (canSkipMetric(name, histogram.count())) {
			return;
		}
		final Snapshot snapshot = histogram.getSnapshot();
		Object[] p = pointsHistogram[0];
		p[0] = influxdb.convertTimestamp(timestamp);
		p[1] = snapshot.size();
		p[2] = histogram.min();
		p[3] = histogram.max();
		p[4] = histogram.mean();
		p[5] = histogram.stdDev();
		p[6] = snapshot.getMedian();
		p[7] = snapshot.get75thPercentile();
		p[8] = snapshot.get95thPercentile();
		p[9] = snapshot.get99thPercentile();
		p[10] = snapshot.get999thPercentile();
		p[11] = histogram.count();
		assert (p.length == COLUMNS_HISTOGRAM.length);
		influxdb.appendSeries(prefix, name, ".histogram", COLUMNS_HISTOGRAM, pointsHistogram);
	}

  @Override
  public void processCounter(MetricName name, Counter counter, Long timestamp) throws IOException {
    reportCounter(name.getName(), counter, timestamp);
  }

	private void reportCounter(String name, Counter counter, long timestamp) {
		Object[] p = pointsCounter[0];
		p[0] = influxdb.convertTimestamp(timestamp);
		p[1] = counter.count();
		assert (p.length == COLUMNS_COUNT.length);
		influxdb.appendSeries(prefix, name, ".count", COLUMNS_COUNT, pointsCounter);
	}

  @Override
  public void processGauge(MetricName name, Gauge gauge, Long timestamp) throws IOException {
    reportGauge(name.getName(), gauge, timestamp);
  }

	private void reportGauge(String name, Gauge<?> gauge, long timestamp) {
		Object[] p = pointsGauge[0];
		p[0] = influxdb.convertTimestamp(timestamp);
		p[1] = gauge.value();
		assert (p.length == COLUMNS_GAUGE.length);
		influxdb.appendSeries(prefix, name, ".value", COLUMNS_GAUGE, pointsGauge);
	}

  @Override
  public void processMeter(MetricName name, Metered meter, Long timestamp) throws IOException {
    reportMeter(name.getName(), meter, timestamp);
  }

  private void reportMeter(String name, Metered meter, long timestamp) {
		if (canSkipMetric(name, meter.count())) {
			return;
		}
		Object[] p = pointsMeter[0];
		p[0] = influxdb.convertTimestamp(timestamp);
		p[1] = meter.count();
		p[2] = meter.oneMinuteRate();
		p[3] = meter.fiveMinuteRate();
		p[4] = meter.fifteenMinuteRate();
		p[5] = meter.meanRate();
		assert (p.length == COLUMNS_METER.length);
		influxdb.appendSeries(prefix, name, ".meter", COLUMNS_METER, pointsMeter);
	}
}
