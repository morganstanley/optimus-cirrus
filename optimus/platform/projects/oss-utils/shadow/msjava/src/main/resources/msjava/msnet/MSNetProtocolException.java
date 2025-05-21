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
import msjava.tools.util.MSOctalEncoder;
public class MSNetProtocolException extends MSNetException {
    protected boolean isFatal;
    protected MSNetMessage response;
    protected byte[] messageBytes;
    
    
    public MSNetProtocolException() {
        this("<unknown>", true);
    }
    
    public MSNetProtocolException(String message, byte[] messageBytes) {
        this(message, messageBytes, true);
        this.messageBytes = messageBytes;
    }
    public MSNetProtocolException(String message, byte[] messageBytes, boolean isFatal) {
        super("MSNetProtocolException: " + message + encodeMessageBytes(messageBytes));
        this.isFatal = isFatal;
        this.messageBytes = messageBytes;
    }
    
    public MSNetProtocolException(String message, boolean isFatal) {
        this(message, null, isFatal);
    }
    private static String encodeMessageBytes(byte[] bytes) {
        if (bytes == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder(40);
        sb.append(" (received: '");
        int i = 0;
        while (i < bytes.length && i < 40) {
            char c = (char) (bytes[i] & 0xff );
            if (c == '\\') {
                sb.append("\\\\");
            } else if (c >= 0x20 && c < 0x7F) {
                sb.append(c);
            } else {
                sb.append("\\").append(MSOctalEncoder.asOctal(bytes[i]));
            }
            ++i;
        }
        if (i!=bytes.length){
            sb.append("...");
        }
        sb.append("')");
        
        return sb.toString();
    }
    
    public boolean isFatal() {
        return isFatal;
    }
    
    public boolean isResponseSet() {
        return (response != null);
    }
    
    public MSNetMessage getResponse() {
        return response;
    }
    
    public void setResponse(MSNetMessage response) {
        this.response = response;
    }
}
