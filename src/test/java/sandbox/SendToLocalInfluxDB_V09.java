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
package sandbox;

import java.util.concurrent.TimeUnit;

import com.yammer.metrics.core.*;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.reporting.ConsoleReporter;

import metrics2_influxdb.HttpInfluxdbProtocol;
import metrics2_influxdb.InfluxdbReporter;
import metrics2_influxdb.UdpInfluxdbProtocol;

public class SendToLocalInfluxDB_V09 {

	public static void main(String[] args) {
		AbstractPollingReporter r0 = null;
		AbstractPollingReporter r1 = null;
		AbstractPollingReporter r2 = null;
		try {
			final MetricsRegistry registry = new MetricsRegistry();
			r0 = startConsoleReporter(registry);
			r1 = startInfluxdbReporterHttpV09(registry);
			r2 = startInfluxdbReporterUDPV09(registry);

			// TODO what to do with NaN & infinity
			registerGaugeWithValues(registry, "integer", Integer.MIN_VALUE, Integer.MAX_VALUE, 1);
			registerGaugeWithValues(registry, "long", Long.MIN_VALUE, Long.MAX_VALUE, 1l);
			registerGaugeWithValues(registry, "double", Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 1.0d);
			registerGaugeWithValues(registry, "float", Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, 1.0f);

			final Meter mymeter0 = registry.newMeter(
			    new MetricName("", "MyMeter","0"),
          "event-type",
          TimeUnit.MILLISECONDS);
			for (int i = 0; i < 100; i++) {
				mymeter0.mark();
				mymeter0.mark(Math.round(Math.random() * 100.0));
				Thread.sleep(Math.round(Math.random() * 1000.0));
			}
		} catch (Exception exc) {
			exc.printStackTrace();
			System.exit(1);
		} finally {
			if (r2 != null) {
				r2.run();
				r2.shutdown();
			}
			if (r1 != null) {
				r1.run();
				r1.shutdown();
			}
			if (r0 != null) {
				r0.run();
				r0.shutdown();
			}
			System.out.println("STOP");
		}
	}

	private static AbstractPollingReporter startInfluxdbReporterUDPV09(MetricsRegistry registry) {
		final AbstractPollingReporter reporter = InfluxdbReporter.forRegistry(registry)
				.protocol(new UdpInfluxdbProtocol("127.0.0.1", 8089))
				.build();
		reporter.start(20, TimeUnit.SECONDS);
		return reporter;
	}

	private static AbstractPollingReporter startInfluxdbReporterHttpV09(MetricsRegistry registry) {
		final AbstractPollingReporter reporter = InfluxdbReporter.forRegistry(registry)
				.protocol(new HttpInfluxdbProtocol("127.0.0.1", 8086, "test", "u0", "u0PWD"))
				.build();
		reporter.start(10, TimeUnit.SECONDS);
		return reporter;
	}

	private static void registerGaugeWithValues(MetricsRegistry registry, String prefix, Object... values) {
		for (final Object value : values) {
		  registry.newGauge(
		      new MetricName("", "", prefix + value),
          new Gauge<Object>() {
		        @Override
            public Object value() { return value; }});
		}
	}

	private static ConsoleReporter startConsoleReporter(MetricsRegistry registry) throws Exception {
	  ConsoleReporter reporter = new ConsoleReporter(registry, System.out, MetricPredicate.ALL);
		reporter.start(1, TimeUnit.MINUTES);
		return reporter;
	}
}
