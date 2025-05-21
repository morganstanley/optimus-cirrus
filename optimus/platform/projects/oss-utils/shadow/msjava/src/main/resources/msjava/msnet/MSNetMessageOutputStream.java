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
import java.io.IOException;
import msjava.msnet.utils.MSNetConfiguration;
public class MSNetMessageOutputStream<T extends MSNetProtocolTCPConnection> extends MSNetBufferedMessageOutputStream<T> {
    public static final int UNKNOWN_WRITE_SIZE = -1;
    private int _fragmentSize = MSNetConfiguration.DEFAULT_OUTPUTSTREAM_FRAGMENT_SIZE;
    
    public static final int PREVENT_MESSAGE_FRAGMENTATION = -1; 
    MSNetMessageOutputStream(T conn_, int fragmentSize_) {
        super(conn_);
        _fragmentSize = fragmentSize_;
        if (isNonStreamingMessage) {
            throw new MSNetRuntimeException("Attempting to use fragmentation with a protocol " + conn_.getProtocol()
                    + "that does not support streaming. Use MSNetBufferedMessageOutputStream instead");
        }
    }
    
    @Override
    public void write(int b) throws IOException {
        super.write(b);
        if (_fragmentSize > PREVENT_MESSAGE_FRAGMENTATION && _len >= _fragmentSize) {
            flush();
        }
    }
    @Override
    public void write(byte[] b_, int offset_, int len_) throws IOException {
        super.write(b_, offset_, len_);
        if (_fragmentSize > PREVENT_MESSAGE_FRAGMENTATION && _len >= _fragmentSize) {
            flush();
        }
    }
    @Override
    public void flush() throws IOException {
        if (sendBufferedMessage()) {
            
            checkForError();
        }
    }
    public int getFragmentSize() {
        return _fragmentSize;
    }
    public void setFragmentSize(int fragmentSize_) {
        _fragmentSize = fragmentSize_;
    }
    @Override
    protected void cleanupOnClose() {
        _conn.closeMessageOutputStream(); 
    }
}
