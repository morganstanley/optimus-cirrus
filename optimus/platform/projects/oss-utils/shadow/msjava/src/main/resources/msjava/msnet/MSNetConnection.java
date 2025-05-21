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
import java.util.Date;
import javax.annotation.CheckReturnValue;
import msjava.msnet.auth.MSNetAuthContext;
public interface MSNetConnection extends AutoCloseable {
    
    public static final int InfiniteRetries = -1;
    
    public static final int DefaultMaxRetryTime = 30 * 1000;
    
    public static final int InfiniteDisconnects = -1;
    
    public static final int DefaultMaxDisconnects = InfiniteDisconnects;
    
    
    public static final long DefaultInitialRetryTime = 1000;
    
    public void connect();
    
    public void connect(MSNetAddress address);
    
    @Override
    public void close();
    
    public void reparent(MSNetLoop loop);
    
    public int send(MSNetMessage message);
    
    public int send(byte[] message);
    
    @CheckReturnValue
    public boolean syncConnect(long millisec);
    
    @CheckReturnValue
    public boolean syncConnect(MSNetAddress address, long millisec);
    
    public MSNetIOStatus syncRead(MSNetMessage message, long millisec);
    
    public MSNetIOStatus syncSend(MSNetMessage message, long millisec);
    
    public MSNetIOStatus syncSend(byte[] message, int size, long millisec);
    
    public void asyncSend(byte[] message, int size);
    
    public void asyncSend(MSNetMessage message);
    
    public MSNetAddress getAddress();
    
    public MSNetAddress getLocalAddress();
    
    public boolean setAddress(MSNetAddress address);
    
    public MSNetAddress getBackupAddress();
    
    public void setBackupAddress(MSNetAddress address);
    
    public MSNetAddress getPrimaryAddress();
    
    public void setPrimaryAddress(MSNetAddress address);
    
    public Date connectTime();
    
    public MSNetConnectionStateEnum getState();
    
    public String getInfoMessage();
    
    public MSNetAuthContext getAuthContext();
    
    public void addListener(MSNetConnectionListener listener);
    
    public void removeListener(MSNetConnectionListener listener);
    
    public boolean getRetryFlag();
    
    public void setRetryFlag(boolean retryFlag);
    
    public long getRetryTime();
    
    public void setRetryTime(long time);
    
    public long getMaxRetryTime();
    
    public void setMaxRetryTime(long time);
    
    public int getMaxRetries();
    
    public void setMaxRetries(int retries);
    
    public int getMaxDisconnects();
    
    public void setMaxDisconnects(int maxDisconnects);
    
    public MSNetID getName();
    
    public void setName(MSNetID id);
    
    public MSNetLoop getNetLoop();
    
    public void enableReading();
    
    public void disableReading();
    
    public boolean isReadingEnabled();
    
    public boolean isConnected();
    
    public boolean isEstablished();
    
    public boolean getTapping();
    
    public void setTapping(boolean tapping);
};

This line added to force a compiler error.  This code should only be built as part of open source external build.