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

package msjava.zkapi.internal;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.zookeeper.data.Stat;
import org.yaml.snakeyaml.Yaml;
public final class ZkaData implements Map<String,Object> {
    
    @Deprecated
    public static final String          ID           = "id";
    
    public static final String          RESERVED_KEY = "__RESERVED";
    
    private static final boolean CREATE_RESERVED_ID = true;
    private final Map<String, Object> data = new HashMap<>();
    {
    	ConcurrentHashMap<String, Object> reserved = new ConcurrentHashMap<>();
    	if (CREATE_RESERVED_ID) {
    	    reserved.put(ID, 1L);
    	}
        data.put(RESERVED_KEY, reserved);
    }
    
    public ZkaData() {}
    public static ZkaData fromBytes(byte[] data) {
        return new ZkaData(data);
    }
    @SuppressWarnings("unchecked")
    public static Optional<ZkaData> tryFromBytes(byte[] data) {
        if (data != null) {
            Object obj = new Yaml().load(new ByteArrayInputStream(data));
            if (obj instanceof Map)
                return Optional.of(new ZkaData((Map<String, Object>) obj));
        }
        return Optional.empty();
    }
    
    @SuppressWarnings("unchecked")
    private ZkaData(byte[] bytes) {
        Object obj = new Yaml().load(new ByteArrayInputStream(bytes));
        if(obj instanceof Map) {
            data.putAll((Map<? extends String, ? extends Object>) obj);
        } else {
            
            put("string", String.valueOf(obj));
        }
    }
    public ZkaData(ChildData cd) {
        this(cd.getData());
    }
    public ZkaData(Map<String,Object> data) {
        if(data != null) {
            this.data.putAll(data);
        }
    }
    
    public Map<String,Object> getRawMap() {
        return new HashMap<>(data);
    }
    
    
    @Deprecated
    public Stat getStat() {
        return new Stat();
    }
    @Override
    public synchronized String toString() {
        return "ZkaData" + data;
    }
    public synchronized byte[] serialize() {
        
        return toBytes(getRawMap());
    }
    
    private static byte[] toBytes(Map<String, Object> payload) {
        return new Yaml().dump(payload).getBytes(StandardCharsets.UTF_8);
    } 
     
    @SuppressWarnings("unchecked")
    public synchronized Map<String,Object> getReservedMap() {
        return (Map<String,Object>) data.get(RESERVED_KEY);
    }
    
    @Override
    public synchronized int size() {
        return data.size();
    }
    
    @Override
    public synchronized boolean isEmpty() {
        
        assert data.size() > 0;
        return data.size() == 1;
    }
    
    @Override
    public synchronized boolean containsKey(Object key) {
        return data.containsKey(key);
    }
    
    @Override
    public synchronized boolean containsValue(Object value) {
        return data.containsValue(value);
    }
    
    @Override
    public synchronized Object get(Object key) {
        if(key.equals(RESERVED_KEY)) {
            return new HashMap<>(getReservedMap());
        }
        return data.get(key);
    }
    
    @Override
    public synchronized Object put(String key, Object value) {
        if(key.equals(RESERVED_KEY)) 
            throw new IllegalArgumentException("Can't assign value to the reserved map.");
        return data.put(key, value);
    }
    
    @Override
    public synchronized Object remove(Object key) {
        if(key.equals(RESERVED_KEY)) {
            return null;
        }
        return data.remove(key);
    }
    
    @Override
    public synchronized void putAll(Map<? extends String, ? extends Object> m) {
        final Map<String, Object> zka = getReservedMap();
        data.putAll(m); 
        data.put(RESERVED_KEY, zka);
    }
    public synchronized void replaceWith(Map<? extends String, ? extends Object> m) {
        final Map<String, Object> zka = getReservedMap();
        data.clear();
        data.putAll(m);
        data.put(RESERVED_KEY, zka);
    }
    
    @Override
    public synchronized void clear() {
        final Map<String, Object> zka = getReservedMap();
        data.clear();
        data.put(RESERVED_KEY, zka);
    }
    
    @Override
    public synchronized Set<String> keySet() {
        return data.keySet();
    }
    
    @Override
    public synchronized Collection<Object> values() {
        return data.values();
    }
    
    @Override
    public synchronized Set<Map.Entry<String, Object>> entrySet() {
        return data.entrySet();
    }
    
    @Override
    public synchronized boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ZkaData other = (ZkaData) obj;
        return data.equals(other.data);
    }
    
    @Override
    public int hashCode() {
        return data.hashCode();
    }
    
    @Deprecated
    public synchronized long getId() {
        return 1L;
    }
    
    @Deprecated
    public synchronized int getInteger(String key) {
        return Integer.parseInt(getReservedMap().get(key).toString());
    }
}
