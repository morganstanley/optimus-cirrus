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
public class MSNetTCPServerMetrics implements Cloneable {
    int _numDisconnects;
    long _lastDisconnect;
    int _numConnects;
    long _lastConnect;
    int _numDisconnectsSweep;
    long _lastDisconnectSweep;
    int _numConnectsSweep;
    long _lastConnectSweep;
    public MSNetTCPServerMetrics() {
        
    }
    public synchronized void resetSweep() {
        _numDisconnectsSweep = 0;
        _numConnectsSweep = 0;
    }
    public synchronized Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException x) {
            
            x.printStackTrace();
        }
        return null;
    }
    public synchronized final void connectionAccepted() {
        _lastDisconnect = System.currentTimeMillis();
        ++_numConnects;
        ++_numConnectsSweep;
    }
    public synchronized final void connectionDropped() {
        _lastConnect = System.currentTimeMillis();
        ++_numDisconnects;
        ++_numDisconnectsSweep;
    }
    public final int getNumDisconnects() {
        return _numDisconnects;
    }
    public final long getLastDisconnect() {
        return _lastDisconnect;
    }
    public final int getNumConnects() {
        return _numConnects;
    }
    public final long getLastConnect() {
        return _lastConnect;
    }
    public final int getNumDisconnectsSweep() {
        return _numDisconnectsSweep;
    }
    public final int getNumConnectsSweep() {
        return _numConnectsSweep;
    }
    
    public void clearLifetimeMetrics() {
        _numDisconnects = _numDisconnectsSweep;
        _lastDisconnect = _lastDisconnectSweep;
        _numConnects = _numConnectsSweep;
        _lastConnect = _lastConnectSweep;
    }
}
