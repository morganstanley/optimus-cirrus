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
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import msjava.msnet.utils.MSNetConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public abstract class MSNetByteBufferManager {
    static final Logger LOG = LoggerFactory.getLogger(MSNetByteBufferManager.class);
    
    static final int MIN_BUFFER_SIZE = MSNetConfiguration.MIN_SOCKETBUFFER_FRAGMENT_SIZE;
    
    private static final MSNetByteBufferManager INSTANCE;
    
    static {
        if (MSNetConfiguration.ALLOW_DIRECT_TCP_SOCKET_BUFFER){
            INSTANCE = new SimpleByteBufferManager();
        } else {
            INSTANCE = new NeverByteBufferManager();
        }
    }
    
    public static MSNetByteBufferManager getInstance() {
        return INSTANCE;
    }
    
    public abstract ByteBuffer getBuffer(int bufferSize, boolean direct);
    public abstract void returnBuffer(ByteBuffer bb);
    public abstract ByteBuffer getDefaultDirectBuffer();
    public abstract void returnDefaultDirectBuffer(ByteBuffer bb);
    
    private abstract static class AbstractMSNetByteBufferManager extends MSNetByteBufferManager {
        protected static final int THREADLOCAL_DIRECTBUFFE_SIZE = Math.max(MSNetConfiguration.RECEIVE_BUFFER_SIZE,
                MSNetConfiguration.WRITE_BUFFER_SIZE);
        private ThreadLocal<ByteBuffer> threadLocalDirectBuffer = new ThreadLocal<ByteBuffer>() {
            @Override
            protected ByteBuffer initialValue() {
                return ByteBuffer.allocateDirect(THREADLOCAL_DIRECTBUFFE_SIZE);
            }
        };
        @Override
        public ByteBuffer getDefaultDirectBuffer() {
            ByteBuffer byteBuffer = threadLocalDirectBuffer.get();
            return byteBuffer;
        }
        @Override
        public void returnDefaultDirectBuffer(ByteBuffer bb) {
            assert bb == threadLocalDirectBuffer.get();
            bb.clear();
        }
    }
    
    private static class SimpleByteBufferManager extends AbstractMSNetByteBufferManager {
        @Override
        public ByteBuffer getBuffer(int bufferSize, boolean direct) {
            if (direct) {
                return ByteBuffer.allocateDirect(Math.max(MIN_BUFFER_SIZE, bufferSize));
            } else {
                return ByteBuffer.allocate(Math.max(MIN_BUFFER_SIZE, bufferSize));
            }
        }
        @Override
        public void returnBuffer(ByteBuffer bb) {
            
        }
    }
    
    private static class NeverByteBufferManager extends AbstractMSNetByteBufferManager {
        @Override
        public ByteBuffer getBuffer(int bufferSize, boolean direct) {
            
            return ByteBuffer.allocate(Math.max(MIN_BUFFER_SIZE, bufferSize));
        }
        @Override
        public void returnBuffer(ByteBuffer bb) {
            
        }
    }
    
    
    
    @SuppressWarnings("unused")
    private static class PoolingByteBufferManager extends MSNetByteBufferManager {
        
        private static final int DEFAULT_BUFFER_SIZE = 128 * 1024; 
        
        static int MAX_BUFFERCOUNT = 0; 
        Queue<ByteBuffer> pool = new ConcurrentLinkedQueue<ByteBuffer>();
        AtomicInteger nonDirectCount = new AtomicInteger(0);
        Semaphore limit = new Semaphore(MAX_BUFFERCOUNT);
        private PoolingByteBufferManager() {
            LOG.debug("Allocating direct buffer pool");
            long start = System.currentTimeMillis();
            for (int i = MAX_BUFFERCOUNT; i > 0; --i) {
                pool.add(allocateDirect(DEFAULT_BUFFER_SIZE));
            }
            LOG.debug("Direct buffer pool allocated in " + (System.currentTimeMillis() - start) + " + ms");
        }
        @Override
        public ByteBuffer getBuffer(int bufferSize, boolean direct) {
            
            if (limit.tryAcquire()) {
                ByteBuffer peek = pool.poll();
                if (null != peek) {
                    peek.clear();
                    
                    return peek;
                }
                
                
                
                return allocateDirect(bufferSize);
            }
            
            
            int n;
            if ((n = nonDirectCount.incrementAndGet()) % 1000 == 1) {
                
                LOG.warn("Non direct buffer returned (" + n + ")");
            }
            return ByteBuffer.allocate(Math.max(MIN_BUFFER_SIZE, bufferSize));
        }
        public void returnBuffer(ByteBuffer bb) {
            if (!bb.isDirect()) {
                return;
            }
            
            limit.release();
            pool.add(bb);
        }
        public void returnDefaultDirectBuffer(ByteBuffer bb) {
        }
        public ByteBuffer getDefaultDirectBuffer() {
            return null;
        }
        private static ByteBuffer allocateDirect(int size) {
            ByteBuffer bb = ByteBuffer.allocateDirect(size);
            
            
            return bb;
        }
    }
}
