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
package msjava.base.slr.internal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
public class ServiceLookup {
    private static final Logger              log            = LoggerFactory.getLogger(ServiceLookup.class);
    private static final Map<String, String> NO_LOOKUP_KEYS = Collections.emptyMap();
    private  String meta;
    private  String service;
    final private Map<String, String> lookupKeys;
    private final Object key;
    
    public ServiceLookup(String name, Map<String, String> lookupKeys) {
        initMetaService(name);
        this.lookupKeys = lookupKeys == null ? NO_LOOKUP_KEYS : toImmutableMap(lookupKeys);
        this.key = createKey();
    }
    
    public ServiceLookup(String meta, String service, Map<String, String> lookupKeys) {
        this.meta = meta;
        this.service = service;
        this.lookupKeys = lookupKeys == null ? NO_LOOKUP_KEYS : toImmutableMap(lookupKeys);
        this.key = createKey();
    }
    
    private Map<String, String> toImmutableMap(Map<String, String> keys) {
        final HashMap<String, String> map = new HashMap<String, String>(keys.size() * 2);
        map.putAll(keys);
        return Collections.unmodifiableMap(map);
    }
    private void initMetaService(String name) {
        if (name != null) {
            if (!name.contains("/")) {
                this.meta = "";
                this.service = name;
            } else {
                this.meta = name.substring(0, name.lastIndexOf('/'));
                this.service = name.substring(name.lastIndexOf('/') + 1);
            }
            log.debug("meta {} service {}", meta, service);
        }
    }
    
    private Object createKey() {
        final StringBuilder b = new StringBuilder();
        final char sep = (char) 0;
        b.append(meta).append(sep).append(service).append(sep);
        for (final Entry<String, String> e : lookupKeys.entrySet()) {
            b.append(e.getKey()).append(sep).append(e.getValue()).append(sep);
        }
        return b.toString();
    }
    public static ServiceLookup forName(String name) {
        return new ServiceLookup(name, NO_LOOKUP_KEYS);
    }
    public String getMeta() {
        return meta;
    }
    public String getService() {
        return service;
    }
    public Map<String, String> getLookupKeys() {
        return lookupKeys;
    }
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        if (StringUtils.isEmpty(meta)) {
            builder.append(service);
        } else {
            builder.append(meta).append("/").append(service);
        }
        if (lookupKeys != null && !lookupKeys.isEmpty()) {
            builder.append(";").append(lookupKeys);
        }
        return builder.toString();
    }
    
    public String getName() {
        if (StringUtils.isEmpty(getMeta())) {
            return getService();
        }
        return getMeta() + "/" + getService();
    }
    @Override
    public int hashCode() {
        return key.hashCode();
    }
    @Override
    public boolean equals(Object obj) {
        return obj instanceof ServiceLookup && ((ServiceLookup) obj).key.equals(key);
    }
}
