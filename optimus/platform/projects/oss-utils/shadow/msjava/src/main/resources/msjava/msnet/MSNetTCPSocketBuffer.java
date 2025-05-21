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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import msjava.msnet.utils.MSNetConfiguration;
public class MSNetTCPSocketBuffer {
    static final MSNetByteBufferManager BUFFER_MANAGER_INSTANCE = MSNetByteBufferManager.getInstance();
    
    protected int head = 0;
    
    protected int size = 0;
    
    protected final MSNetArrayDeque<ByteBuffer> buffers = new MSNetArrayDeque<ByteBuffer>();
    
    private boolean lastBufferInMessage = true;
    private final boolean direct;
    final class ReadViewIterator implements Iterator<ByteBuffer> {
        private final int start;
        private final int end;
        
        int offset = 0;
        ByteBuffer nextBuffer = null;
        int bit = 0;
        private ReadViewIterator(int start, int length) {
            this.start = start;
            this.end = start + length;
            if (length == 0) {
                
                return;
            }
            
            ByteBuffer bb = buffers.get(bit++);
            offset += bb.capacity();
            while (offset <= start && bit < buffers.size()) {
                bb = buffers.get(bit++);
                offset += bb.capacity();
            }
            if (offset <= start) {
                bb = null; 
            } else {
                int capacity = bb.capacity();
                int limit = Math.min(end - offset + capacity, capacity);
                int position = start - offset + capacity;
                nextBuffer = (ByteBuffer) bb.duplicate();
                nextBuffer.limit(limit);
                nextBuffer.position(position);
            }
        }
        @Override
        public boolean hasNext() {
            return nextBuffer != null;
        }
        @Override
        public ByteBuffer next() {
            if (nextBuffer == null) {
                throw new NoSuchElementException();
            }
            ByteBuffer toReturn = nextBuffer;
            if (offset >= end || bit >= buffers.size()) {
                nextBuffer = null;
            } else {
                ByteBuffer bb = buffers.get(bit++);
                int capacity = bb.capacity();
                int limit = Math.min(end - offset, capacity);
                int position = Math.max(start - offset, 0);
                nextBuffer = (ByteBuffer) bb.duplicate();
                nextBuffer.limit(limit);
                nextBuffer.position(position);
                offset += capacity;
            }
            return toReturn;
        }
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
    final class TCPSocketBufferInputStream extends InputStream {
        ReadViewIterator readViewIterator;
        int position = 0;
        int mark = 0;
        
        ByteBuffer currentBuffer;
        private TCPSocketBufferInputStream() {
            readViewIterator = getReadViewIterator();
            currentBuffer = readViewIterator.next();
        }
        @Override
        public int read() throws IOException {
            if (!currentBuffer.hasRemaining()) {
                if (!readViewIterator.hasNext()) {
                    return -1;
                }
                currentBuffer = readViewIterator.next();
            }
            ++position;
            return currentBuffer.get() & 0xff;
        }
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int remBytes = len;
            
            while (remBytes != 0){
                int remBuffer = currentBuffer.remaining();
                if (remBuffer == 0) {
                    if (!readViewIterator.hasNext()) {
                        int copied = len - remBytes;
                        return copied == 0 ? -1 : copied;
                    }
                    currentBuffer = readViewIterator.next();
                    remBuffer = currentBuffer.remaining();
                }
                int toCopy = Math.min(remBuffer, remBytes); 
                currentBuffer.get(b, off + len - remBytes, toCopy);
                remBytes -= toCopy;
                position += toCopy;
            }
            assert remBytes == 0;
            return len;
        }
        
        @Override
        public synchronized void mark(int readlimit) {
            mark = position;
        }
        @Override
        public synchronized void reset() throws IOException {
            position = mark;
            
            readViewIterator = getReadViewIterator();
            
            int p = position;
            while ((currentBuffer = readViewIterator.next()).remaining() < p) {
                p -= currentBuffer.remaining();
            }
            
            currentBuffer.position(currentBuffer.position() + p);
        }
        
        @Override
        public boolean markSupported() {
            return true;
        }
        @Override
        public int available() throws IOException {
            return size() - position;
        }
        
    }
    private static int sumRemaining(List<ByteBuffer> buffers) {
        int s = 0;
        for (ByteBuffer byteBuffer : buffers) {
            s += byteBuffer.remaining();
        }
        return s;
    }
    
    public MSNetTCPSocketBuffer() {
        this.direct = MSNetConfiguration.isSocketBufferDirect();
        assert invariant();
    }
    
    public MSNetTCPSocketBuffer(boolean direct) {
        this.direct = direct;
        assert invariant();
    }
    
    public MSNetTCPSocketBuffer(ByteBuffer buf) {
        direct = MSNetConfiguration.isSocketBufferDirect();
        head = buf.position();
        size = buf.remaining();
        ByteBuffer buffer = (ByteBuffer) buf.duplicate().flip();
        buffer.limit(buffer.capacity());
        buffer.position(size);
        
        buffers.add(buffer);
        assert invariant();
    }
    
    public MSNetTCPSocketBuffer(MSNetTCPSocketBuffer buffer) {
        this(buffer, true, 0, buffer.size);
    }
    
    public MSNetTCPSocketBuffer(MSNetTCPSocketBuffer buffer, boolean copy) {
        this(buffer, copy, 0, buffer.size);
    }
    
    public MSNetTCPSocketBuffer(MSNetTCPSocketBuffer b, boolean copy, int offset, int length) {
        assert (offset >= 0);
        assert (length <= b.size);
        direct = b.direct;
        if (!copy) {
            size = length;
            head = b.head + offset;
            int o = 0;
            
            int droppedBefore = 0;
            for (int i = 0; i < b.buffers.size(); ++i) {
                ByteBuffer bb = b.buffers.get(i);
                o += bb.capacity();
                if (o <= head) {
                    
                    
                    droppedBefore += bb.capacity();
                    BUFFER_MANAGER_INSTANCE.returnBuffer(bb);
                } else if (o - bb.capacity() >= head + length) {
                    
                    BUFFER_MANAGER_INSTANCE.returnBuffer(bb);
                } else {
                    
                    buffers.add(bb);
                }
            }
            
            head -= droppedBefore;
            
            b.buffers.clear();
            b.head = 0;
            b.size = 0;
            assert invariant();
        } else {
            
            store(b.getReadViews(b.head + offset, length));
        }
    }
    
    public MSNetTCPSocketBuffer(MSNetMessage msg) {
        this(msg.getBytes());
    }
    
    public MSNetTCPSocketBuffer(byte[] bytes) {
        this(bytes, 0, bytes.length, MSNetConfiguration.isAdoptBuffers());
    }
    
    public MSNetTCPSocketBuffer(byte[] bytes, int offset, int length) {
        this(bytes, offset, length, MSNetConfiguration.isAdoptBuffers());
    }
    
    public MSNetTCPSocketBuffer(byte[] bytes, int offset, int length, boolean adopt) {
        direct = MSNetConfiguration.isSocketBufferDirect();
        setBytes(bytes, offset, length, adopt);
    }
    
    public void store(List<ByteBuffer> buffersToStore) {
        int rem = sumRemaining(buffersToStore);
        if (rem == 0) {
            
            return;
        }
        MSNetArrayDeque<ByteBuffer> buffers = this.buffers;
        ByteBuffer targetBuffer;
        if (buffers.isEmpty()) {
            targetBuffer = BUFFER_MANAGER_INSTANCE.getBuffer(rem, direct);
            buffers.add(targetBuffer);
        } else {
            targetBuffer = buffers.getLast();
        }
        size += rem;
        Iterator<ByteBuffer> it = buffersToStore.iterator();
        ByteBuffer toStore = it.next(); 
        for (;;) {
            if (targetBuffer.remaining() >= toStore.remaining()) {
                
                rem -= toStore.remaining();
                targetBuffer.put(toStore);
                assert !toStore.hasRemaining();
                if (!it.hasNext()) {
                    
                    assert rem == 0 : rem;
                    break;
                }
                toStore = it.next();
                if (!targetBuffer.hasRemaining()) {
                    targetBuffer = BUFFER_MANAGER_INSTANCE.getBuffer(rem, direct);
                    buffers.add(targetBuffer);
                }
            } else {
                
                rem -= targetBuffer.remaining();
                int limit = targetBuffer.remaining() + toStore.position();
                ByteBuffer limited = (ByteBuffer) toStore.duplicate().limit(limit);
                targetBuffer.put(limited);
                toStore.position(limit); 
                assert !targetBuffer.hasRemaining();
                targetBuffer = BUFFER_MANAGER_INSTANCE.getBuffer(rem, direct);
                buffers.add(targetBuffer);
            }
        }
        assert invariant();
    }
    
    public void store(MSNetTCPSocketBuffer toStore) {
        toStore.moveTo(this, toStore.size, false);
    }
    
    public void store(ByteBuffer buffer) {
        store(buffer, true);
    }
    
    public void store(ByteBuffer buffer, boolean copy){
        int rem = buffer.remaining();
        if (rem == 0) {
            
            return;
        }
        MSNetArrayDeque<ByteBuffer> buffers = this.buffers;
        size += rem;
        if(copy) {
            ByteBuffer targetBuffer;
            if (buffers.isEmpty()) {
                targetBuffer = BUFFER_MANAGER_INSTANCE.getBuffer(rem, direct);
                buffers.add(targetBuffer);
            } else {
                targetBuffer = buffers.getLast();
            }
            for (; ; ) {
                if (targetBuffer.remaining() >= buffer.remaining()) {
                    
                    rem -= buffer.remaining();
                    targetBuffer.put(buffer);
                    assert !buffer.hasRemaining();
                    break;
                } else {
                    
                    rem -= targetBuffer.remaining();
                    int limit = targetBuffer.remaining() + buffer.position();
                    ByteBuffer limited = (ByteBuffer) buffer.duplicate().limit(limit);
                    targetBuffer.put(limited);
                    buffer.position(limit); 
                    assert !targetBuffer.hasRemaining();
                    targetBuffer = BUFFER_MANAGER_INSTANCE.getBuffer(rem, direct);
                    buffers.add(targetBuffer);
                }
            }
        }else {
            if(direct != buffer.isDirect()){
                String bufferType =  buffer.isDirect() ? "direct" : "non-direct";
                String socketBufferType =  buffer.isDirect() ? "direct" : "non-direct";
                throw new IllegalArgumentException(String.format("%s buffer attempted to be stored in the %s MSNetTCPSocketBuffer", bufferType, socketBufferType));
            }
            ByteBuffer duplicate = buffer.duplicate();
            duplicate.position(duplicate.limit());
            buffers.add(duplicate);
        }
        assert invariant();
    }
    
    public void store(byte[] buffer) {
        store(buffer, 0, buffer.length);
    }
    
    public void store(byte[] buffer, int offset, int length) {
        
        
        if (length == 0) {
            return;
        }
        ByteBuffer wrapped = ByteBuffer.wrap(buffer, offset, length);
        store(wrapped);
    }
    
    public void store(byte b) {
        ByteBuffer target;
        if (buffers.isEmpty() || !buffers.getLast().hasRemaining()) {
            target = BUFFER_MANAGER_INSTANCE.getBuffer(1, direct);
            buffers.add(target);
        } else {
            target = buffers.getLast();
        }
        target.put(b);
        ++size;
    }
    
    public void setBytes(byte[] buffer) {
        setBytes(buffer, 0, buffer.length, MSNetConfiguration.isAdoptBuffers());
    }
    
    public void setBytes(byte[] buffer, int offset, int length) {
        setBytes(buffer, offset, length, MSNetConfiguration.isAdoptBuffers());
    }
    
    public void setBytes(byte[] buffer, int offset, int length, boolean adopt) {
        clear();
        if (length == 0) {
            return;
        }
        if (adopt) {
            ByteBuffer wrapped = ByteBuffer.wrap(buffer, offset, length).slice();
            wrapped.flip();
            buffers.add(wrapped);
            size = length;
            assert invariant();
        } else {
            store(buffer, offset, length);
        }
    }
    private byte[] getBytes(int numBytes, int start) {
        byte[] peekData = new byte[numBytes];
        return getBytes(peekData, numBytes, start, 0);
    }
    public byte[] getBytes(byte[] peekData, int numBytes, int start, int arrayOffset) {
        int o = 0;
        int r = 0;
        MSNetArrayDeque<ByteBuffer> buffs = buffers;
        int it = 0;
        while (r < numBytes && o < start + numBytes && it < buffs.size()) {
            ByteBuffer bb = buffs.get(it++);
            int capacity = bb.capacity();
            if (o + capacity > start) {
                
                bb = (ByteBuffer) bb.duplicate();
                bb.limit(capacity);
                bb.position(Math.max(start - o, 0));
                int bytesToCopy = Math.min(bb.remaining(), numBytes - r);
                bb.get(peekData, r + arrayOffset, bytesToCopy);
                r += bytesToCopy;
            }
            o += capacity;
        }
        assert r == numBytes;
        return peekData;
    }
    
    public byte[] getBytes() {
        byte b[] = new byte[capacity()];
        int o = 0;
        for (int i = 0; i < buffers.size(); ++i) {
            ByteBuffer bb = buffers.get(i);
            bb = (ByteBuffer) bb.duplicate().clear();
            bb.get(b, o, bb.remaining());
            o += bb.capacity();
        }
        assert o == capacity();
        return b;
    }
    ArrayList<ByteBuffer> getReadViews() {
        return getReadViews(head, size);
    }
    ArrayList<ByteBuffer> getReadViews(int start, int length) {
        ArrayList<ByteBuffer> r = new ArrayList<ByteBuffer>(buffers.size());
        int offset = 0;
        int end = start + length;
        for (int i = 0; i < buffers.size(); ++i) {
            ByteBuffer bb = buffers.get(i);
            int remainingBytes = end - offset;
            int capacity = bb.capacity();
            if (remainingBytes <= 0) {
                break;
            }
            if (offset + capacity > start) {
                int limit = Math.min(remainingBytes, capacity);
                int position = Math.max(0, start - offset);
                ByteBuffer readView = (ByteBuffer) bb.duplicate();
                readView.limit(limit);
                readView.position(position);
                r.add(readView);
            }
            offset += capacity;
        }
        return r;
    }
    ReadViewIterator getReadViewIterator() {
        return getReadViewIterator(head, size);
    }
    ReadViewIterator getReadViewIterator(final int start, final int length) {
        return new ReadViewIterator(start, length);
    }
    
    public byte[] peek() {
        return peek(size);
    }
    
    public byte[] peek(int numBytes) {
        int start = this.head;
        return getBytes(numBytes, start);
    }
    
    public byte[] tail(int numBytes) {
        return getBytes(numBytes, head + size - numBytes);
    }
    
    public byte[] retrieve() {
        if (size == 0) {
            
            return new byte[0];
        }
        byte ret[] = retrieve(size);
        size = 0;
        return ret;
    }
    
    public byte[] retrieve(int numBytes) {
        byte[] data = peek(numBytes);
        processed(numBytes);
        return data;
    }
    public void retrieve(byte buf[], int offset, int len) {
        getBytes(buf, len, head, offset);
        processed(len);
    }
    
    public void clear() {
        clear(false);
    }
    
    public void clear(boolean releaseMemory) {
        for (int i = 0; i < buffers.size(); ++i) {
            ByteBuffer bb = buffers.get(i);
            BUFFER_MANAGER_INSTANCE.returnBuffer(bb);
        }
        buffers.clear();
        head = 0;
        size = 0;
    }
    
    public int capacity() {
        int r = 0;
        for (int i = 0; i < buffers.size(); ++i) {
            ByteBuffer bb = buffers.get(i);
            r += bb.capacity();
        }
        return r;
    }
    
    public int size() {
        return size;
    }
    
    public boolean isEmpty() {
        return size == 0;
    }
    
    public void processed(int numBytesProcessed) {
        processed(numBytesProcessed, true);
    }
    public void processed(int numOfBytesProcessed, boolean tryToShrinkBuffer) {
        head += numOfBytesProcessed;
        size -= numOfBytesProcessed;
        if (tryToShrinkBuffer) {
            
            shrink();
        }
    }
    
    public MSNetTCPSocketBuffer duplicate() {
        return new MSNetTCPSocketBuffer(this);
    }
    
    public void ensureCapacity(int requiredSize) {
        if (size == 0) {
            clear();
            return;
        }
        shrink();
    }
    private void shrink() {
        
        while (!buffers.isEmpty()) {
            ByteBuffer first = buffers.getFirst();
            int capacity = first.capacity();
            if (capacity > head) {
                break;
            }
            buffers.removeFirst();
            head -= capacity;
            BUFFER_MANAGER_INSTANCE.returnBuffer(first);
        }
    }
    public int getHeadPosition() {
        return head;
    }
    
    boolean isLastBufferInMessage() {
        return lastBufferInMessage;
    }
    
    public void setIsLastBufferInMessage(boolean val) {
        lastBufferInMessage = val;
    }
    public boolean isDirect() {
        return direct;
    }
    void setSize(int i) {
        size = i;
    }
    public String toString() {
        return "MSNetTCPSocketBuffer(" + hashCode() + "): head=" + getHeadPosition() + " direct=" + isDirect()
                + " size=" + size() + "/" + capacity();
    }
    
    public InputStream createReadStream() {
        if (size == 0) {
            return new ByteArrayInputStream(new byte[0]);
        }
        return new TCPSocketBufferInputStream();
    }
    
    boolean invariant() {
        assert size >= 0 : "negative buffer size: " + size;
        
        for (int i = 0; i < buffers.size(); ++i) {
            ByteBuffer byteBuffer = buffers.get(i);
            if (byteBuffer.hasRemaining() && i != buffers.size() - 1) {
                assert false : "i=" + i;
            }
        }
        return true;
    }
    
    int moveTo(MSNetTCPSocketBuffer targetBuffer, int num2copy, boolean moveOnlyFull) {
        assert num2copy <= size : "size=" + size + " num2copy=" + num2copy;
        if (targetBuffer.isEmpty()) {
            
            targetBuffer.clear();
        }
        if (targetBuffer.isEmpty() || (head == 0 && !targetBuffer.buffers.getLast().hasRemaining())) {
            
            while (!buffers.isEmpty() && (buffers.getFirst().capacity() - head <= num2copy)) {
                ByteBuffer b = buffers.removeFirst();
                int move = b.capacity() - head;
                if (move > 0) {
                    targetBuffer.buffers.add(b);
                    size -= move;
                    targetBuffer.size += move;
                    num2copy -= move;
                    if (head != 0) {
                        
                        assert targetBuffer.size == move;
                        targetBuffer.head = head;
                    }
                    head = 0;
                } else {
                    head -= b.capacity();
                    BUFFER_MANAGER_INSTANCE.returnBuffer(b);
                }
            }
            assert invariant();
            assert targetBuffer.invariant();
            if (!isEmpty() && !moveOnlyFull) {
                ByteBuffer first = buffers.getFirst();
                if (buffers.getFirst().position() - head == num2copy) {
                    assert this.buffers.size() == 1;
                    assert first.position() - head == num2copy;
                    
                    if (head != 0) {
                        targetBuffer.head = head;
                    }
                    buffers.removeFirst();
                    size = 0;
                    head = 0;
                    targetBuffer.buffers.add(first);
                    targetBuffer.size += num2copy;
                } else {
                    int limit = num2copy + head;
                    int position = head;
                    ByteBuffer readView = (ByteBuffer) first.duplicate();
                    readView.limit(limit);
                    readView.position(position);
                    assert limit - position == num2copy;
                    targetBuffer.store(readView);
                    processed(num2copy);
                }
                num2copy = 0;
                assert invariant();
                assert targetBuffer.invariant();
            }
            return num2copy;
        } else {
            
            targetBuffer.store(getReadViews(head, num2copy));
            processed(num2copy);
            return 0;
        }
    }
    int getDirecBytesOnStart(int allowedNonDirectBytes) {
        int directbytes = 0;
        int h = head;
        for (int i = 0; i < buffers.size(); ++i) {
            ByteBuffer b = buffers.get(i);
            int capacity = b.capacity();
            int bytes;
            if (h >= capacity) {
                h -= capacity;
                continue;
            } else {
                bytes = capacity - h;
                h = 0;
            }
            if (b.isDirect()) {
                directbytes += bytes;
            } else {
                allowedNonDirectBytes -= bytes;
                if (allowedNonDirectBytes < 0) {
                    return directbytes;
                }
            }
            h -= bytes;
        }
        return directbytes;
    }
    void perpend(byte[] bytes) {
        if (isEmpty()) {
            store(bytes);
            return;
        }
        
        shrink();
        int b = bytes.length;
        if (head > 0) {
            
            ByteBuffer first = buffers.get(0);
            
            int position = first.position();
            int limit = first.limit();
            first.limit(first.capacity());
            int p = Math.max(0, head - b);
            first.position(p);
            int copy = head - p;
            first.put(bytes, b - copy, copy);
            b -= copy;
            head = head - copy;
            size += copy;
            first.position(position);
            first.limit(limit);
        }
        while (b > 0) {
            ByteBuffer targetBuffer = BUFFER_MANAGER_INSTANCE.getBuffer(b, direct);
            buffers.addFirst(targetBuffer);
            int capacity = targetBuffer.capacity();
            head = Math.max(0, capacity - b);
            targetBuffer.position(head);
            int copy = Math.min(capacity - head, capacity);
            targetBuffer.put(bytes, b - copy, copy);
            b -= copy;
            size += copy;
        }
        assert invariant();
    }
}
