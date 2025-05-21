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

package msjava.msnet;
public class MSNetConnectionStateEnum implements MSNetState {
    protected final int enumVal;
    protected final String str;
    protected static final java.util.HashMap<Integer, MSNetConnectionStateEnum> hash = new java.util.HashMap<Integer, MSNetConnectionStateEnum>();
    
    public static final MSNetConnectionStateEnum INVALID = new MSNetConnectionStateEnum(1, "Invalid");
    
    public static final MSNetConnectionStateEnum CONNECTING = new MSNetConnectionStateEnum(2, "Connecting");
    
    public static final MSNetConnectionStateEnum ESTABLISHING = new MSNetConnectionStateEnum(3, "Establishing");
    
    public static final MSNetConnectionStateEnum CONNECTED = new MSNetConnectionStateEnum(4, "Connected");
    
    public static final MSNetConnectionStateEnum DISCONNECTED = new MSNetConnectionStateEnum(5, "Disconnected");
    
    public static final MSNetConnectionStateEnum ABANDONED = new MSNetConnectionStateEnum(6, "Abandoned");
    protected MSNetConnectionStateEnum(int enumVal, String str) {
        this.enumVal = enumVal;
        this.str = str;
        hash.put(enumVal, this);
    }
    
    public String toString() {
        return str;
    }
    
    public int hashCode() {
        return enumVal;
    }
    
    public int enumValue() {
        return enumVal;
    }
    
    public static MSNetConnectionStateEnum forInt(int i) {
        return hash.get(i);
    }
}
