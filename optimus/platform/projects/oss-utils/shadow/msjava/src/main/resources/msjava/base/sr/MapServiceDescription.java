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
package msjava.base.sr;
import static msjava.base.sr.ServiceAttributes.CommonKey.authMethods;
import static msjava.base.sr.ServiceAttributes.CommonKey.host;
import static msjava.base.sr.ServiceAttributes.CommonKey.meta;
import static msjava.base.sr.ServiceAttributes.CommonKey.port;
import static msjava.base.sr.ServiceAttributes.CommonKey.service;
import static msjava.base.sr.ServiceAttributes.CommonKey.url;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import msjava.base.lang.Functions2;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ForwardingMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
public class MapServiceDescription extends ForwardingMap<String, Object> implements ServiceDescription {
    private Map<String, Object> map     = new HashMap<String, Object>();
    private boolean             mutable = true;
    
    public MapServiceDescription() {
    }
    
    
    public MapServiceDescription(Map<String, Object> map) {
        this.map.putAll(map);
    }
    public MapServiceDescription makeImmutable() {
        this.mutable = false;
        return setBackingMap(Collections.unmodifiableMap(map));
    }
    
    
    private MapServiceDescription setBackingMap(Map<String, Object> map) {
        this.map = map;
        return this;
    }
    
    @Override
    protected final Map<String, Object> delegate() {
        return map;
    }
    
    @Override
    public String getEndpointUrl() {
        return (String) get(CommonKey.url);
    }
    public void setEndpointUrl(String endpointUrl) {
        putOrRemove(CommonKey.url, endpointUrl);
    }
    @Override
    public String getHost() {
        return (String) get(host);
    }
    @Override
    public int getPort() {
        return orDefault((Integer) get(port), 0);
    }
    private int orDefault(Integer integer, int dflt) {
        return integer == null? dflt: integer.intValue();
    }
    public void setHost(String host) {
        putOrRemove(CommonKey.host, host);
    }
    public void setPort(int port) {
        put(CommonKey.port, port);
    }
    @Override
    public boolean hasEndpointUrl() { return containsKey(url); }
    @Override
    public boolean hasHostPort() { return containsKey(host); }
    @Override
    public String getMeta() {
        return (String) get(meta);
    }
    public void setMeta(String meta) {
           putOrRemove(CommonKey.meta, meta);
    }
    @Override
    public String getService() {
        return (String) get(service);
    }
    public void setService(String service) {
        putOrRemove(CommonKey.service, service);
    }
    @Override
    @SuppressWarnings("unchecked")
    public Set<String> getAuthMethods() {
        
        return (Set<String>) MoreObjects.firstNonNull(get(authMethods), Collections.emptySet());
    }
    public void setAuthMethods(Set<String> authMethods) {
        putOrRemove(CommonKey.authMethods, authMethods == null ? null : ImmutableSet
                .copyOf(Iterables.transform(authMethods, Functions2.stringToLowerCase(Locale.US))));
    }
    @Override
    public String toString() {
        return getMeta()+"/"+getService()+"=" + (getEndpointUrl() != null ? getEndpointUrl() : "hostport:" + getHost() + ":" + getPort());
    }
    public MapServiceDescription with(CommonKey k, Object v) {
        putOrRemove(k, v);
        return this;
    }
    public MapServiceDescription with(String k, Object v) {
        putOrRemove(k, v);
        return this;
    }
    public MapServiceDescription withService(String service) {
        setService(service);
        return this;
    }
    public MapServiceDescription withEndpointUrl(String endpointUrl) {
        setEndpointUrl(endpointUrl);
        return this;
    }
    
    public void put(CommonKey k, Object v) {
        map.put(k.name(), v);
    }
    private void putOrRemove(CommonKey k, Object v) {
        if (v == null) {
            remove(k);
        } else {
            put(k, v);
        }
    }
    private void putOrRemove(String k, Object v) {
        if (v == null) {
            remove(k);
        } else {
            put(k, v);
        }
    }
    public void remove(String k) {
        remove((Object) k);
    }
    public void remove(CommonKey k) {
        remove(k.name());
    }
    @Override
    public Object get(CommonKey key) {
        return get(key.name());
    }
    
    @Override
    public Object get(String keyName) {
        return get((Object)keyName);
    }
    @Override
    public boolean containsKey(CommonKey key) {
        return containsKey(key.name());
    }
    
    @Override
    public boolean containsKey(String keyName) {
        return containsKey((Object)keyName);
    }
    public MapServiceDescription putAll(ServiceAttributes attributes) {
        if (attributes != null) {
            for (String key : attributes.keySet()) {
                putOrRemove(key, attributes.get(key));
            }
        }
        
        return this;
    }
    
    @Override
    public Object getId() {
        Object id = get(CommonKey.id);
        if (id != null) return id;
        id = getEndpointUrl();
        if (id != null) return id;
        return toHostPort(this);
    }
    public void setId(Object id) {
        putOrRemove(CommonKey.id, id);
    }
    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (!(o instanceof ServiceDescription)) return false;
        if (getId() == null) return false; 
        
        return getId().equals(((ServiceDescription)o).getId());
                
    }
    @Override
    public int hashCode() {
        return getId() == null?
                super.hashCode():
                getId().hashCode();
    }
    
    public static ServiceDescription asMutable(ServiceDescription sd) {
        if (sd instanceof MapServiceDescription && ((MapServiceDescription)sd).mutable) {
            return sd;
        }
        return new MapServiceDescription(sd);
    }
    
    private static String toHostPort(ServiceDescription sd) {
        if (sd.getHost() == null) return null;
        return sd.getHost() + ":" + sd.getPort();
    }
}
