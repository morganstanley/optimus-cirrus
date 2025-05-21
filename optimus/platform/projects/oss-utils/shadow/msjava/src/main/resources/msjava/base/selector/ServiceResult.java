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

package msjava.base.selector;
import msjava.base.annotation.Experimental;
@Experimental
public final class ServiceResult {
    public enum ServiceResultType {
        
        
        FAILED_TO_CONNECT,
        
        DISCONNECTED,
        
        TIMED_OUT,
        
        UNKNOWN_FAILURE,
        
        SUCCESS;
        
    }
    public ServiceResult() {}
    
    
    public ServiceResult(ServiceResultType serviceResultTypeToUse) {
    	
    	assert serviceResultTypeToUse != null;
    	this.serviceResultType = serviceResultTypeToUse;
    }
    
    
    public static ServiceResult newSuccessResult() {
    	return new ServiceResult(ServiceResultType.SUCCESS);
    }
    
    private ServiceResultType serviceResultType;
    public ServiceResultType getServiceResultType() {
        return serviceResultType;
    }
    
    public void setServiceResultType(ServiceResultType serviceResultType) {
    	assert serviceResultType != null;
    	this.serviceResultType = serviceResultType;
    }
    
    private long latency;
    @Override
    public String toString() {
        return String.format("ServiceResult [type=%s]", serviceResultType);
    }
    
    public long getLatency() {
        return latency;
    }
    
    public void setLatency(long latency) {
        this.latency = latency;
    }
    
    public ServiceResult withLatency(long latency) {
        this.latency = latency;
        return this;
    }
}
