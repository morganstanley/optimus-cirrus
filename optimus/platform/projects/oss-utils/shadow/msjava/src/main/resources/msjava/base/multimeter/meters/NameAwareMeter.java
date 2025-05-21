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
import msjava.base.multimeter.Counter;
import msjava.base.multimeter.Gauge;
import msjava.base.multimeter.GaugeSet;
import msjava.base.multimeter.Meter;
import msjava.base.multimeter.ThroughputMonitor;
import msjava.base.multimeter.Timer;
import msjava.base.multimeter.util.MeterUtils;
import msjava.base.lang.StringUtils;
import org.springframework.util.Assert;
public abstract class NameAwareMeter implements Meter {
	
	private final String className;
	
	protected NameAwareMeter(String className) {
		Assert.hasText(className, "'className' cannot be empty");
		this.className = className;
	}
	
	
	public String getClassName() {
		return className;
	}
	
	protected String getFullPlugName(String plugName) {
		Assert.hasText(plugName, "'plugName' cannot be empty");
		return String.format("%s.%s", className, plugName);
	}
	
	public String getAggregateName(String plugName, Map<String, String> tags) {
		String fullName = getFullPlugName(plugName);
		if (tags == null)
			return fullName;
		Map<String, String> validatedTags = validateTags(tags);
		String attributes = StringUtils.join(getMapValues(validatedTags), "", "[", ",", "]");
		return fullName + attributes;
	}
	
	private List<String> getMapValues(Map<String, String> map) {
		List<String> result = new ArrayList<String>();
		for (String key : map.keySet())
			result.add(map.get(key));
		return result;
	}
	private Map<String, String> validateTags(Map<String, String> tags) throws IllegalArgumentException {
		Map<String, String> result = new HashMap<String, String>();
		for (String key : tags.keySet()) {
			String value = tags.get(key);
			if (MeterUtils.isValidTagValue(value)) {
				result.put(key, value);
			} else {
				throw new IllegalArgumentException("Tag value rejected: " + value);
			}
		}
		return result;
	}
	
	
	
	@Override
    public abstract Timer getTimer(final String shortName);
	@Override
    public abstract <T> void registerGauge(final String shortName, final Gauge<T> logic);
	
	@Override
    public abstract void registerGaugeSet(final GaugeSet<Gauge<?>> gaugeSet);
	@Override
    public abstract Counter getCounter(final String shortName);
	@Override
    public abstract ThroughputMonitor getThroughputMonitor(final String shortName);
		
	
	
	public static abstract class Builder<T extends NameAwareMeter> {
		
		public abstract T buildMeter(String className);
		
		public abstract String getImplementationName();
	}
}
