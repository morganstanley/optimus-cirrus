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
public class MSNetTCPConnectionMetrics implements Cloneable {
    
    long _creationTime = System.currentTimeMillis();
    int _numDisconnects;
    long _lastDisconnect;
    int _numConnects;
    long _lastConnect;
    int _numRetries;
    long _lastRetry;
    long _totalBytesSent;
    long _minBytesSent;
    long _maxBytesSent;
    int _numSends;
    int _numReceives;
    long _totalBytesReceived;
    long _minBytesReceived;
    long _maxBytesReceived;
    long _lastReceive;
    int _numRetriesSweep;
    long _totalBytesSentSweep;
    long _minBytesSentSweep;
    long _maxBytesSentSweep;
    int _numSendsSweep;
    int _numReceivesSweep;
    long _totalBytesReceivedSweep;
    long _minBytesReceivedSweep;
    long _maxBytesReceivedSweep;
    public MSNetTCPConnectionMetrics() {
        
    }
    public MSNetTCPConnectionMetrics(MSNetTCPConnectionMetrics other_) {
        _numDisconnects = other_._numDisconnects;
        _lastDisconnect = other_._lastDisconnect;
        _numConnects = other_._numConnects;
        _lastConnect = other_._lastConnect;
        _numRetries = other_._numRetries;
        _lastRetry = other_._lastRetry;
        _totalBytesSent = other_._totalBytesSent;
        _minBytesSent = other_._minBytesSent;
        _maxBytesSent = other_._maxBytesSent;
        _numSends = other_._numSends;
        _totalBytesReceived = other_._totalBytesReceived;
        _minBytesReceived = other_._minBytesReceived;
        _maxBytesReceived = other_._maxBytesReceived;
        _numReceives = other_._numReceives;
        _lastReceive = other_._lastReceive;
    }
    public synchronized void resetSweep() {
        _numRetriesSweep = 0;
        _totalBytesSentSweep = 0;
        _minBytesSentSweep = 0;
        _maxBytesSentSweep = 0;
        _numSendsSweep = 0;
        _numReceivesSweep = 0;
        _totalBytesReceivedSweep = 0;
        _minBytesReceivedSweep = 0;
        _maxBytesReceivedSweep = 0;
    }
    
    public synchronized void clearLifetimeMetrics() {
        _numRetries = _numRetriesSweep;
        _totalBytesSent = _totalBytesSentSweep;
        _minBytesSent = _minBytesSentSweep;
        _maxBytesSent = _maxBytesSentSweep;
        _numSends = _numSendsSweep;
        _numReceives = _numReceivesSweep;
        _totalBytesReceived = _totalBytesReceivedSweep;
        _minBytesReceived = _minBytesReceivedSweep;
        _maxBytesReceived = _maxBytesReceivedSweep;
        _numDisconnects = 0;
        _lastDisconnect = 0;
        _numConnects = 0;
        _lastConnect = 0;
        _numRetries = 0;
        _lastRetry = 0;
    }
    public synchronized Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException x) {
            
            x.printStackTrace();
        }
        return null;
    }
    public synchronized final void gotDisconnected() {
        ++_numDisconnects;
        _lastDisconnect = System.currentTimeMillis();
    }
    public synchronized final void gotConnected() {
        ++_numConnects;
        _lastConnect = System.currentTimeMillis();
    }
    public synchronized final void sentBytes(int numBytesSent_) {
        
        ++_numSends;
        _totalBytesSent += numBytesSent_;
        if ((numBytesSent_ < _minBytesSent) || (0 == _minBytesSent)) {
            _minBytesSent = numBytesSent_;
        }
        if (numBytesSent_ > _maxBytesSent) {
            _maxBytesSent = numBytesSent_;
        }
        
        ++_numSendsSweep;
        _totalBytesSentSweep += numBytesSent_;
        if ((numBytesSent_ < _minBytesSentSweep) || (0 == _minBytesSentSweep)) {
            _minBytesSentSweep = numBytesSent_;
        }
        if (numBytesSent_ > _maxBytesSentSweep) {
            _maxBytesSentSweep = numBytesSent_;
        }
    }
    public synchronized final void receivedBytes(int numBytesReceived_) {
        
        ++_numReceives;
        _totalBytesReceived += numBytesReceived_;
        if ((numBytesReceived_ < _minBytesReceived) || (0 == _minBytesReceived)) {
            _minBytesReceived = numBytesReceived_;
        }
        if (numBytesReceived_ > _maxBytesReceived) {
            _maxBytesReceived = numBytesReceived_;
        }
        _lastReceive = System.currentTimeMillis();
        
        ++_numReceivesSweep;
        _totalBytesReceivedSweep += numBytesReceived_;
        if ((numBytesReceived_ < _minBytesReceivedSweep) || (0 == _minBytesReceivedSweep)) {
            _minBytesReceivedSweep = numBytesReceived_;
        }
        if (numBytesReceived_ > _maxBytesReceivedSweep) {
            _maxBytesReceivedSweep = numBytesReceived_;
        }
    }
    public synchronized final void didRetry() {
        
        ++_numRetries;
        _lastRetry = System.currentTimeMillis();
        
        ++_numRetriesSweep;
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
    public final long getCreationTime() {
        return _creationTime;
    }
    public final long getLastConnect() {
        return _lastConnect;
    }
    public final long getLastRetry() {
        return _lastRetry;
    }
    public final int getNumRetries() {
        return _numRetries;
    }
    public final int getNumSends() {
        return _numSends;
    }
    public final long getTotalBytesSent() {
        return _totalBytesSent;
    }
    public final long getMinBytesSent() {
        return _minBytesSent;
    }
    public final long getMaxBytesSent() {
        return _maxBytesSent;
    }
    public final int getNumReceives() {
        return _numReceives;
    }
    public final long getTotalBytesReceived() {
        return _totalBytesReceived;
    }
    public final long getMinBytesReceived() {
        return _minBytesReceived;
    }
    public final long getMaxBytesReceived() {
        return _maxBytesReceived;
    }
    public final long getLastReceive() {
        return _lastReceive;
    }
    public final int getNumRetriesSweep() {
        return _numRetriesSweep;
    }
    public final int getNumSendsSweep() {
        return _numSendsSweep;
    }
    public final long getTotalBytesSentSweep() {
        return _totalBytesSentSweep;
    }
    public final long getMinBytesSentSweep() {
        return _minBytesSentSweep;
    }
    public final long getMaxBytesSentSweep() {
        return _maxBytesSentSweep;
    }
    public final int getNumReceivesSweep() {
        return _numReceivesSweep;
    }
    public final long getTotalBytesReceivedSweep() {
        return _totalBytesReceivedSweep;
    }
    public final long getMinBytesReceivedSweep() {
        return _minBytesReceivedSweep;
    }
    public final long getMaxBytesReceivedSweep() {
        return _maxBytesReceivedSweep;
    }
}
