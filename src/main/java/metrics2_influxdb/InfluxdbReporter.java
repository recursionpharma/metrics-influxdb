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
package metrics2_influxdb;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.AbstractPollingReporter;

import metrics2_influxdb.api.measurements.MetricMeasurementTransformer;
import metrics2_influxdb.measurements.HttpInlinerSender;
import metrics2_influxdb.measurements.MeasurementReporter;
import metrics2_influxdb.measurements.Sender;
import metrics2_influxdb.measurements.UdpInlinerSender;
import metrics2_influxdb.misc.Miscellaneous;
import metrics2_influxdb.misc.VisibilityIncreasedForTests;
import metrics2_influxdb.v08.Influxdb;
import metrics2_influxdb.v08.InfluxdbHttp;
import metrics2_influxdb.v08.InfluxdbUdp;
import metrics2_influxdb.v08.ReporterV08;

/**
 * A reporter which publishes metric values to a InfluxDB server.
 *
 * @see <a href="http://influxdb.org/">InfluxDB - An open-source distributed
 *      time series database with no external dependencies.</a>
 */
public class InfluxdbReporter  {

	static enum InfluxdbCompatibilityVersions {
		V08, LATEST;
	}

	/**
	 * Returns a new {@link Builder} for {@link InfluxdbReporter}.
	 *
	 * @param registry
	 *          the registry to report
	 * @return a {@link Builder} instance for a {@link InfluxdbReporter}
	 */
	public static Builder forRegistry(MetricsRegistry registry) {
		return new Builder(registry);
	}

	/**
	 * A builder for {@link InfluxdbReporter} instances. Defaults to not using a
	 * prefix, using the default clock, converting rates to events/second,
	 * converting durations to milliseconds, and not filtering metrics.
	 */
	public static class Builder {

		private final MetricsRegistry registry;
		private Clock clock;
		private String prefix;
		private boolean skipIdleMetrics;

		@VisibilityIncreasedForTests InfluxdbCompatibilityVersions influxdbVersion;
		@VisibilityIncreasedForTests InfluxdbProtocol protocol;
		@VisibilityIncreasedForTests Influxdb influxdbDelegate;
		@VisibilityIncreasedForTests Map<String, String> tags;
		@VisibilityIncreasedForTests MetricMeasurementTransformer transformer = MetricMeasurementTransformer.NOOP;

		private Builder(MetricsRegistry registry) {
			this.registry = registry;
			this.clock = Clock.defaultClock();
			this.prefix = null;
			this.protocol = new HttpInfluxdbProtocol();
			this.influxdbVersion = InfluxdbCompatibilityVersions.LATEST;
			this.tags = new HashMap<>();
		}

		/**
		 * Use the given {@link Clock} instance for the time.
		 *
		 * @param clock a {@link Clock} instance
		 * @return {@code this}
		 */
		public Builder withClock(Clock clock) {
			this.clock = clock;
			return this;
		}

		/**
		 * Prefix all metric names with the given string.
		 *
		 * @param prefix the prefix for all metric names
		 * @return {@code this}
		 */
		public Builder prefixedWith(String prefix) {
			this.prefix = prefix;
			return this;
		}

		/**
		 * Only report metrics that have changed.
		 *
		 * @param skipIdleMetrics
		 * @return {@code this}
		 */
		public Builder skipIdleMetrics(boolean skipIdleMetrics) {
			this.skipIdleMetrics = skipIdleMetrics;
			return this;
		}

		/**
		 * Builds a {@link AbstractPollingReporter} with the given properties, sending
		 * metrics using the given InfluxDB.
		 *
		 * @return a {@link AbstractPollingReporter}
		 */
		public AbstractPollingReporter build() {
			AbstractPollingReporter reporter;

			switch (influxdbVersion) {
			case V08:
				Influxdb influxdb = buildInfluxdb();
				reporter = new ReporterV08(registry, influxdb, clock, prefix, skipIdleMetrics);
				break;
			default:
				Sender s = buildSender();
				reporter = new MeasurementReporter(s, registry, clock, tags, transformer);
			}
			return reporter;
		}

		/**
		 * Operates with influxdb version less or equal than 08.
		 * @return the builder itself
		 */
		public Builder v08() {
			this.influxdbVersion  = InfluxdbCompatibilityVersions.V08;
			return this;
		}

		/**
		 * Override the protocol to use.
		 * @param protocol a non null protocol
		 * @return
		 */
		public Builder protocol(InfluxdbProtocol protocol) {
			Objects.requireNonNull(protocol, "given InfluxdbProtocol cannot be null");
			this.protocol = protocol;
			return this;
		}

		/**
		 * Sets the metric2measurement transformer to be used.
		 * @param transformer a non null transformer
		 * @return
		 */
		public Builder transformer(MetricMeasurementTransformer transformer) {
			Objects.requireNonNull(transformer, "given MetricMeasurementTransformer cannot be null");
			this.transformer = transformer;
			return this;
		}

		/**
		 * Registers the given key/value as a default tag for the generated measurements.
		 * @param tagKey the key to register, cannot be null or empty
		 * @param tagValue the value to register against the given key, cannot be null or empty
		 */
		public Builder tag(String tagKey, String tagValue) {
			Miscellaneous.requireNotEmptyParameter(tagKey, "tag");
			Miscellaneous.requireNotEmptyParameter(tagValue, "value");
			tags.put(tagKey, tagValue);
			return this;
		}

		private Influxdb buildInfluxdb() {
			if (protocol instanceof HttpInfluxdbProtocol) {
				try {
					HttpInfluxdbProtocol p = (HttpInfluxdbProtocol) protocol;
					return new InfluxdbHttp(p.scheme, p.host, p.port, p.database, p.user, p.password, TimeUnit.MILLISECONDS);
				} catch(RuntimeException exc) {
					throw exc;
				} catch(Exception exc) {
					// wrap exception into RuntimeException
					throw new RuntimeException(exc.getMessage(), exc);
				}
			} else if (protocol instanceof UdpInfluxdbProtocol) {
				UdpInfluxdbProtocol p = (UdpInfluxdbProtocol) protocol;
				return new InfluxdbUdp(p.host, p.port);
			} else {
				throw new IllegalStateException("unsupported protocol: " + protocol);
			}
		}

		private Sender buildSender() {
			if (protocol instanceof HttpInfluxdbProtocol) {
				return new HttpInlinerSender((HttpInfluxdbProtocol) protocol);
				// TODO allow registration of transformers
				// TODO evaluate need of prefix (vs tags)
			} else if (protocol instanceof UdpInfluxdbProtocol) {
				return new UdpInlinerSender((UdpInfluxdbProtocol) protocol);
			} else {
				throw new IllegalStateException("unsupported protocol: " + protocol);
			}

		}
	}
}
