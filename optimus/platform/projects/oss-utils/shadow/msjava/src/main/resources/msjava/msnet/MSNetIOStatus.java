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
public class MSNetIOStatus {
    protected int numBytesInMessage;
    protected int numBytesProcessed;
    protected MSNetException exception;
    
    private boolean isWriteErrorInSyncWriteSelectLoop;
    
    private boolean isReadErrorInSyncWriteSelectLoop;
    
    public MSNetIOStatus() {
        numBytesInMessage = numBytesProcessed = 0;
        exception = null;
    }
    public MSNetIOStatus(MSNetException e) {
        numBytesInMessage = numBytesProcessed = 0;
        exception = e;
    }
    
    public MSNetIOStatus(int numBytesInMessage, int numBytesProcessed) {
        this.numBytesInMessage = numBytesInMessage;
        this.numBytesProcessed = numBytesProcessed;
        exception = null;
    }
    
    public MSNetIOStatus(int numBytesInMessage, int numBytesProcessed, MSNetException exception) {
        this.numBytesInMessage = numBytesInMessage;
        this.numBytesProcessed = numBytesProcessed;
        this.exception = exception;
    }
    
    public MSNetIOStatus(MSNetIOStatus status) {
        numBytesInMessage = status.numBytesInMessage;
        numBytesProcessed = status.numBytesProcessed;
        exception = status.exception;
        isWriteErrorInSyncWriteSelectLoop = status.isWriteErrorInSyncWriteSelectLoop;
        isReadErrorInSyncWriteSelectLoop = status.isReadErrorInSyncWriteSelectLoop;
    }
    
    public boolean inError() {
        return (exception != null);
    }
    public int getNumBytesInMessage() {
        return numBytesInMessage;
    }
    public void setNumBytesInMessage(int num) {
        numBytesInMessage = num;
    }
    public int getNumBytesProcessed() {
        return numBytesProcessed;
    }
    public void setNumBytesProcessed(int num) {
        numBytesProcessed = num;
    }
    
    public void raise() throws MSNetException {
        if (exception != null) {
            throw exception;
        }
    }
    public void setException(MSNetException exception) {
        this.exception = exception;
    }
    public MSNetException getException() {
        return exception;
    }
    public String toString() {
        return "MSNetIOStatus: numBytesInMessage=" + numBytesInMessage + ", numBytesProcessed=" + numBytesProcessed
                + ", exception=" + exception;
    }
    
    void setWriteErrorInSyncWriteSelectLoop(boolean isError) {
        isWriteErrorInSyncWriteSelectLoop = isError;
    }
    
    boolean isWriteErrorInSyncWriteSelectLoop() {
        return isWriteErrorInSyncWriteSelectLoop;
    }
    
    void setReadErrorInSyncWriteSelectLoop(boolean isError) {
        isReadErrorInSyncWriteSelectLoop = isError;
    }
    
    boolean isReadErrorInSyncWriteSelectLoop() {
        return isReadErrorInSyncWriteSelectLoop;
    }
}
