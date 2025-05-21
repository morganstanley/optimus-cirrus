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
public class MSNetConnectionErrorEnum {
    protected final String str;
    protected final int enumVal;
    protected static final java.util.HashMap<Integer, MSNetConnectionErrorEnum> hash = new java.util.HashMap<Integer, MSNetConnectionErrorEnum>();
    public static final MSNetConnectionErrorEnum CONNECT_FAILED = new MSNetConnectionErrorEnum(1, "ConnectFailed");
    public static final MSNetConnectionErrorEnum WRITE_ERROR = new MSNetConnectionErrorEnum(2, "WriteError");
    public static final MSNetConnectionErrorEnum READ_ERROR = new MSNetConnectionErrorEnum(3, "ReadError");
    public static final MSNetConnectionErrorEnum ADDRESS_ERROR = new MSNetConnectionErrorEnum(4, "AddressError");
    public static final MSNetConnectionErrorEnum PROTOCOL_ERROR = new MSNetConnectionErrorEnum(5, "ProtocolError");
    public static final MSNetConnectionErrorEnum SYSTEM_ERROR = new MSNetConnectionErrorEnum(6, "SystemError");
    public static final MSNetConnectionErrorEnum NO_ERROR = new MSNetConnectionErrorEnum(7, "NoError");
    public static final MSNetConnectionErrorEnum PEER_TIMEOUT = new MSNetConnectionErrorEnum(8, "PeerTimeout");
    public static final MSNetConnectionErrorEnum ESTABILISH_TIMEOUT = new MSNetConnectionErrorEnum(9, "EstabilishTimeout");
    protected MSNetConnectionErrorEnum(int enumVal, String str) {
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
    public boolean equals(Object obj_) {
        if (obj_ != null && obj_ instanceof MSNetConnectionErrorEnum) {
            return enumVal == ((MSNetConnectionErrorEnum) obj_).enumValue();
        }
        return false;
    }
    
    public int enumValue() {
        return enumVal;
    }
    
    public static MSNetConnectionErrorEnum forInt(int i) {
        return hash.get(i);
    }
}
