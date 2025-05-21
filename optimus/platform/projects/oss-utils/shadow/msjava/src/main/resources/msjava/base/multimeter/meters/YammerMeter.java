/*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package msjava.base.multimeter.meters;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import msjava.base.annotation.Internal;
import msjava.base.multimeter.Counter;
import msjava.base.multimeter.Gauge;
import msjava.base.multimeter.GaugeSet;
import msjava.base.multimeter.ThroughputMonitor;
import msjava.base.multimeter.Timer;
import msjava.base.multimeter.TimerContext;
import msjava.base.multimeter.meters.plugs.CounterBase;
import msjava.base.multimeter.util.MeterUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
public class YammerMeter extends NameAwareMeter {
    
    private static final ReadWriteLock positionMappingLock = new ReentrantReadWriteLock();
    private static final Map<String, List<String>> positionMapping = new HashMap<String, List<String>>();
    private static MetricRegistry.MetricSupplier<com.codahale.metrics.Timer> defaultTimerSupplier;
    private MetricRegistry.MetricSupplier<com.codahale.metrics.Timer> timerSupplier;
    
    public static void setDefaultTimerSupplier(MetricRegistry.MetricSupplier<com.codahale.metrics.Timer> defaultTimerSupplier){
        YammerMeter.defaultTimerSupplier = defaultTimerSupplier;
    }
    
    public void setTimerSupplier(MetricRegistry.MetricSupplier<com.codahale.metrics.Timer> timerSupplier){
        this.timerSupplier = timerSupplier;
    }
    
    @Internal
    public static void setPositionMapping(Map<String, List<String>> positionMapping) {
        positionMappingLock.writeLock().lock();
        try {
            YammerMeter.positionMapping.clear();
            addPositionMappings(positionMapping);
        } finally {
            positionMappingLock.writeLock().unlock();
        }
    }
    
    public static void addPositionMappings(Map<String, List<String>> map) {
        positionMappingLock.writeLock().lock();
        try {
            for (Entry<String, List<String>> e : map.entrySet()) {
                List<String> list = positionMapping.get(e.getKey());
                if (list != null) {
                    list.removeAll(e.getValue()); 
                    list.addAll(e.getValue());
                } else {
                    positionMapping.put(e.getKey(), new ArrayList<>(e.getValue()));
                }
            }
        } finally {
            positionMappingLock.writeLock().unlock();
        }
    }
	private static final Logger LOGGER = LoggerFactory.getLogger(NameAwareMeter.class);
	private static final MetricRegistry metricRegistry = new MetricRegistry();
	public static MetricRegistry getMetricRegistry() {
		return metricRegistry;
	}
	public YammerMeter(String className) {
		super(className);
	}
	
	
	
	
    @Override
    public Timer getTimer(final String shortName) {
        return new Timer() {
            @Override
            public TimerContext start(Map<String, String> tags) {
                com.codahale.metrics.Timer timer = getOrSupplyDefaultTimer(getAggregateName(shortName, tags));
                com.codahale.metrics.Timer.Context context = timer.time();
                return new YammerTimerContext(context);
            }
            @Override
            public TimerContext start() {
                return start(null);
            }
            @Override
            public <U> U time(Callable<U> callable, Map<String, String> tags) throws Exception {
                com.codahale.metrics.Timer timer = getOrSupplyDefaultTimer(getAggregateName(shortName, tags));
                return timer.time(callable);
            }
            @Override
            public <U> U time(Callable<U> callable) throws Exception {
                return time(callable, null);
            }
            private com.codahale.metrics.Timer getOrSupplyDefaultTimer(String name){
                if(timerSupplier != null){
                    return metricRegistry.timer(name, timerSupplier);
                }
                if(defaultTimerSupplier != null){
                    return metricRegistry.timer(name, defaultTimerSupplier);
                }
                return metricRegistry.timer(name);
            }
        };
    }
	private static class YammerTimerContext implements TimerContext {
		private com.codahale.metrics.Timer.Context context;
		public YammerTimerContext(com.codahale.metrics.Timer.Context context) {
			Assert.notNull(context, "The context mustn't be null.");
			this.context = context;
		}
		
		@Override
		public long stop() {
			return context.stop();
		}
	}
	
	@Override
	public <T> void registerGauge(final String shortName, final Gauge<T> logic) {
		metricRegistry.register(getFullPlugName(shortName), new com.codahale.metrics.Gauge<T>() {
			@Override
			public T getValue() {
				try {
					return logic.call();
				} catch (Exception e) {
					LOGGER.warn("Exception in gauge.", e);
					return null;
				}
			}
		});
	}
	
	@Override
	public Counter getCounter(final String shortName) {
		return new CounterBase() {
			
			@Override
			public void addDelta(long n, Map<String, String> tags)
					throws IllegalArgumentException {
				com.codahale.metrics.Counter counter = metricRegistry
						.counter(getAggregateName(shortName, tags));
				counter.inc(n);
			}
			
		};
	}
	
	@Override
	public ThroughputMonitor getThroughputMonitor(final String shortName) {
		return new ThroughputMonitor() {
			@Override
			public void mark(Map<String, String> tags) {
				com.codahale.metrics.Meter meter = metricRegistry.meter(getAggregateName(shortName, tags));
				meter.mark();
			}
			@Override
			public void mark() {
				mark(null);
				
			}
		};
	}
    @Override
    public String getAggregateName(String plugName, Map<String, String> tags) {
        String fullName = getFullPlugName(plugName);
        if (tags != null && !tags.isEmpty()) {
            List<String> attributes = new ArrayList<String>();
            positionMappingLock.readLock().lock();
            try {
                List<String> pos = positionMapping.get(fullName);
                if (pos == null)
                    throw new IllegalArgumentException(
                            "The position mapping does not contain definitions for the given meter name.");
                for (String p : pos) {
                    String tag = tags.get(p);
                    if (StringUtils.isBlank(tag)) {
                        attributes.add("");
                    } else if (MeterUtils.isValidTagValue(tag)) {
                        attributes.add(tag);
                    } else {
                        throw new IllegalArgumentException("Tag value rejected: " + tag);
                    }
                }
            } finally {
                positionMappingLock.readLock().unlock();
            }
            fullName = fullName + msjava.base.lang.StringUtils.join(attributes, "", "[", ",", "]");
        }
        return fullName;
    }
	
	public static class Builder extends NameAwareMeter.Builder<YammerMeter> {
		
		@Override
		public YammerMeter buildMeter(String className) {
			return new YammerMeter(className);
		}
		@Override
		public String getImplementationName() {
			return "yammer";
		}
	}
	 
    @Override
    public void registerGaugeSet(final GaugeSet<Gauge<?>> gaugeSet) {
        for (Entry<String, Gauge<?>> entry : gaugeSet.getGauges().entrySet())
            this.registerGauge(entry.getKey(), entry.getValue());
    }
    
    
    public Map<String, Metric> getMetrics() {
        Map<String, Metric> result = new HashMap<String, Metric>();
        for (Entry<String, Metric> e : getMetricRegistry().getMetrics().entrySet()) {
            if (e.getKey().startsWith(getClassName())) {
                result.put(e.getKey(), e.getValue());
            }
        }
        return result;
    }
}
