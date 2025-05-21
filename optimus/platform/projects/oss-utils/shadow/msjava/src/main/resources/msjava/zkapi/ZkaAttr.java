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

package msjava.zkapi;
import javax.annotation.Nullable;
import msjava.base.annotation.Experimental;
import msjava.base.sr.ServiceDescription;
import msjava.zkapi.internal.ZkaData;
import msjava.zkapi.leader.LeaderElector;
import msjava.zkapi.leader.internal.ZkaLeaderElector;
import msjava.zkapi.support.ZkaUtils;
public enum ZkaAttr {
    
    
    BUFFER_TIME              ("bufferTime",           int.class, ZkaAttr.DEFAULT_BUFFER_TIME),
    
    SESSION_TIMEOUT          ("sessionTimeout",       int.class, ZkaAttr.DEFAULT_SESSION_TIMEOUT),
    
    LEADER_COUNT             ("leaderCount",          int.class, ZkaAttr.DEFAULT_LEADER_COUNT),
    
    CONNECTION_TIMEOUT       ("connectionTimeout",    int.class, ZkaAttr.DEFAULT_CONNECTION_TIMEOUT),
    
    ABDICATE_ON_PARTITION    ("abdicateOnPartition", boolean.class, false),
    
    UPDATE_LEADER_COUNT      ("updateLeaderCount",    boolean.class, true),
    
    AUTO_START               ("autoStart",            boolean.class, true),
    
    ORDER                    ("order",               int.class, 0),
    
    DEFERENTIAL              ("deferential",         boolean.class, true),
    
    JAIL                    ("jail",                 boolean.class, false),
    
    
    JMX                     ("jmx",                  boolean.class, true),
    
    
    SERVICE                 ("service",              String.class, ""),
    
    
    @Experimental
    KERBEROS                ("kerberos",             boolean.class, true),
    
    
    ADDITIONAL              ("additionalEnsemble",           String.class, ""),
    
    
    @Deprecated
    ZK34                    ("zk34",                 boolean.class, false),
    ;
    
    public static final int          DEFAULT_LEADER_COUNT       = 1;
    public static final int          DEFAULT_BUFFER_TIME        = 0;
    
    public static final String       DEFAULT_REGION             = "na";
    
    public static final int          DEFAULT_CONNECTION_TIMEOUT = 10000;
    
    public static final int          DEFAULT_SESSION_TIMEOUT    = 60000;
	
    public static final String       ALL_SERVICE                = "*"; 
    
    final private String key;
    final private Class<?> type;
    final private Object defaultValue;
    ZkaAttr(String key, Class<?> type, Object defaultValue) {
        this.key = key;
        this.type = type;
        this.defaultValue = defaultValue;
    }
    public Class<?> getType() {
        return type;
    }
    public String getKey() {
        return key;
    }
    public Object getDefaultValue() {
        return defaultValue;
    }
    
    
    
    @Deprecated
    public static boolean serviceMatches(ServiceDescription sd, @Nullable String service) {
        return ZkaUtils.serviceMatches(sd, service);
    }
    
    @Deprecated
    public static boolean serviceMatches(ZkaData zd, @Nullable String service) {
        return ZkaUtils.serviceMatches(zd, service);
    }
}