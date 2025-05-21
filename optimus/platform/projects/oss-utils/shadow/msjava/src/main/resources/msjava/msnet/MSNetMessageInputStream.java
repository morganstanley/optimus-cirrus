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
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.List;
import javax.annotation.concurrent.GuardedBy;
import org.slf4j.Logger;
import msjava.base.slf4j.ContextLogger;
import msjava.msnet.utils.MSNetConfiguration;
public class MSNetMessageInputStream extends InputStream {
    private static final Logger LOGGER = ContextLogger.safeLogger();
    final byte _tmpbuf[] = new byte[1];
    private final MSNetTCPSocketBuffer buf = new MSNetTCPSocketBuffer();
    private boolean error;
    private boolean complete;
    private final MSNetConnection connection;
    private int timeout = 0;
    
    private int sizeFromHeader = -1;
    private final int limit;
    private final int restartLimit;
    private static int calcRestartLimit(int stopLimit) {
        
        if (stopLimit < 0) {
            return -1;
        } else if (stopLimit >= 8 * MSNetConfiguration.MIN_SOCKETBUFFER_FRAGMENT_SIZE) {
            return stopLimit - 2 * MSNetConfiguration.MIN_SOCKETBUFFER_FRAGMENT_SIZE;
        } else if (stopLimit >= 4 * MSNetConfiguration.MIN_SOCKETBUFFER_FRAGMENT_SIZE) {
            return stopLimit - MSNetConfiguration.MIN_SOCKETBUFFER_FRAGMENT_SIZE;
        } else if (stopLimit >= 4) {
            return stopLimit - stopLimit / 4;
        } else {
            return stopLimit - 1;
        }
    }
    
    protected MSNetMessageInputStream(MSNetConnection connection, int limit) {
        assert connection != null;
        assert limit != 0;
        this.connection = connection;
        this.limit = limit;
        this.restartLimit = calcRestartLimit(limit);
    }
    MSNetID getConnectionID() {
        return connection.getName();
    }
    
    protected synchronized void storeBytes(ByteBuffer bbuf, boolean complete) {
        this.complete = complete;
        buf.store(bbuf);
        checkLimits();
        
        notifyAll();
    }
    protected synchronized void storeBytes(List<ByteBuffer> bbuf, boolean complete) {
        this.complete = complete;
        buf.store(bbuf);
        checkLimits();
        
        notifyAll();
    }
    synchronized void setError(boolean error) {
        this.error = error;
        notifyAll();
    }
    @Override
    public int available() {
        return buf.size();
    }
    @Override
    public synchronized int read(byte b[], int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        long end = System.currentTimeMillis() + timeout;
        
        while (buf.isEmpty() && !complete && !error) {
            
            try {
                if (timeout == 0) {
                    
                    wait();
                } else {
                    wait(end - System.currentTimeMillis());
                }
            } catch (InterruptedException e) {
                throw new InterruptedIOException("Exception waiting for input: " + e);
            }
            if (timeout > 0 && System.currentTimeMillis() >= end) {
                throw new SocketTimeoutException("Timeout exceeded (" + timeout + "ms)");
            }
        }
        if (buf.isEmpty()) {
            if (complete) {
                return -1;
            } else if (error) {
                throw new EOFException("Error reading from protocol stream");
            } else {
                
            }
        }
        
        int availLen = Math.min(len, buf.size());
        buf.retrieve(b, off, availLen);
        checkLimits();
        
        assert availLen != 0;
        return availLen;
    }
    @Override
    public int read() throws IOException {
        int c = read(_tmpbuf, 0, 1);
        if (c == -1) {
            return c;
        }
        return (_tmpbuf[0] & 0xff);
    }
    
    public int getTimeout() {
        return timeout;
    }
    
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
    public int getSizeFromHeader() {
        return sizeFromHeader;
    }
    public void setSizeFromHeader(int sizeFromHeader) {
        this.sizeFromHeader = sizeFromHeader;
    }
    @GuardedBy("this")
    private void checkLimits() {
        if (limit < 0) {
            return;
        }
        if (buf.size() >= limit) {
            pauseReading();
        } else if (buf.size() <= restartLimit) {
            restartReading();
        }
    }
    @GuardedBy("this")
    private void pauseReading() {
        if (connection.isReadingEnabled()) {
            LOGGER.info(
                    "Streaming buffer limit reached - pausing reading. (Current buffer size={}, limit={}, restartLimit={})",
                    buf.size(), limit, restartLimit);
            connection.disableReading();
        }
    }
    @GuardedBy("this")
    private void restartReading() {
        if (!connection.isReadingEnabled()) {
            if (connection.isConnected()) {
                LOGGER.info(
                        "Streaming buffer size dropped below limit - restarting reading. (Current buffer size={}, limit={}, restartLimit={})",
                        buf.size(), limit, restartLimit);
            } else if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        "Streaming buffer size dropped below limit - restarting reading. (Connection is closed.) (Current buffer size={}, limit={}, restartLimit={})",
                        buf.size(), limit, restartLimit);
            }
            connection.enableReading();
        }
    }
}
