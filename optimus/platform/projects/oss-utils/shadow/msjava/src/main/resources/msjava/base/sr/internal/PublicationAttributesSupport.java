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
package msjava.base.sr.internal;
import java.util.Map;
import msjava.base.slr.internal.ServiceName;
import msjava.base.sr.MapServiceDescription;
import msjava.base.sr.ServiceAttributes;
import msjava.base.sr.ServiceAttributes.CommonKey;
import com.google.common.base.Supplier;
public class PublicationAttributesSupport implements Supplier<ServiceAttributes> {
    private MapServiceDescription ref = null;
    
    
    @Override
    public ServiceAttributes get() {
        return ref;
    }
    
    private MapServiceDescription lazilyInitedAttrbutes() {
        if (ref == null)
                ref = new MapServiceDescription();
        return ref; 
    }
    
    
    
    public PublicationAttributesSupport mergeWith(String serviceName) {
        if (serviceName == null) return this;
        
        MapServiceDescription d = lazilyInitedAttrbutes();
        d.remove(CommonKey.meta.name());
        d.remove(CommonKey.service.name());
        
        d.putAll(ServiceName.toServiceDescription(serviceName));
        return this;
    }
    
    
    
    public PublicationAttributesSupport mergeWith(Map<String, Object> toMerge) {
        if (toMerge == null) {
            ref = null;
            return this;
        }
        
        lazilyInitedAttrbutes().putAll(toMerge);
        return this;
    }
    
}