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
package msjava.management.jmx.remote.connector;
import com.sun.jmx.remote.generic.ServerSynchroMessageConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
public class JMXOperation {
    private final String operationName;
    private final List<Object> params;
    private final long invocationTime;
    private final String userName;
    private final UUID uuid;
    private final String callersConnectionID;
    private final String callersINetAddress;
    private final int callersPort;
    public JMXOperation(long time, String userName, String callersConnectionID, String callersINetAddress,
            int callersPort, String operationName, Object[] params) {
        this.invocationTime = time;
        this.userName = userName;
        this.operationName = operationName;
        if (params != null) {
            this.params = Collections.unmodifiableList(new ArrayList<>(Arrays.asList(params)));
        } else {
            this.params = null;
        }
        this.uuid = UUID.randomUUID();
        this.callersConnectionID = callersConnectionID;
        this.callersINetAddress = callersINetAddress;
        this.callersPort = callersPort;
    }
    
    public String getOperationName() {
        return operationName;
    }
    
    public List<Object> getParams() {
        return params;
    }
    
    public long getInvocationTime() {
        return invocationTime;
    }
    
    public String getUserName() {
        return userName;
    }
    
    public UUID getUuid() {
        return uuid;
    }
    
    public String getCallersConnectionID() {
        return callersConnectionID;
    }
    
    public String getCallersINetAddress() {
        return callersINetAddress;
    }
    
    public int getCallersPort() {
        return callersPort;
    }
    @Override
    public String toString() {
        return "JMXOperation{" +
                "uuid=" + uuid +
                ", operationName='" + operationName + '\'' +
                ", params=" + params +
                ", invocationTime=" + invocationTime +
                ", userName='" + userName + '\'' +
                ", callersConnectionID='" + callersConnectionID + '\'' +
                ", callersINetAddress='" + callersINetAddress + '\'' +
                ", callersPort=" + callersPort +
                '}';
    }
}
