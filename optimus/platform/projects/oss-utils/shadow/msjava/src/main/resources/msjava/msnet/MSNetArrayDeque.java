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
import msjava.base.annotation.Internal;
import java.util.Arrays;
@Internal
class MSNetArrayDeque<T> {
    final static int INITIAL_CAPACITY = 16; 
    protected T[] arr;
    protected int head; 
    protected int tail; 
    @SuppressWarnings("unchecked")
    public MSNetArrayDeque() {
        arr = (T[]) new Object[INITIAL_CAPACITY];
    }
    private void doubleCapacity() {
        assert head == tail;
        int p = head;
        int n = arr.length;
        int r = n - p; 
        int newCapacity = n << 1;
        if (newCapacity < 0)
            throw new IllegalStateException("Sorry, deque too big");
        @SuppressWarnings("unchecked")
        T[] a = (T[]) new Object[newCapacity];
        System.arraycopy(arr, p, a, 0, r);
        System.arraycopy(arr, 0, a, r, p);
        arr = a;
        head = 0;
        tail = n;
    }
    public boolean add(T o) {
        assert o != null;
        arr[tail] = o;
        tail = (tail + 1) & (arr.length - 1);
        if (tail == head) {
            doubleCapacity();
        }
        return true;
    }
    
    public boolean addFirst(T o) {
        assert o != null;
        head = head - 1  & (arr.length - 1);
        arr[head] = o;
        if (tail == head) {
            doubleCapacity();
        }
        return true;
    }
    public T get(int i) {
        assert i < arr.length;
        assert i >= 0;
        return arr[(head + i) & (arr.length - 1)];
    }
    public boolean isEmpty() {
        return head == tail;
    }
    public int size() {
        return (tail - head) & (arr.length - 1);
    }
    public void clear() {
        int h = head;
        int t = tail;
        if (h != t) {
            int i = h;
            int mask = arr.length - 1;
            do {
                arr[i] = null;
                i = (i + 1) & mask;
            } while (i != t);
        }
        head = 0;
        tail = 0;
    }
    public T getFirst() {
        assert !isEmpty();
        return arr[head];
    }
    public T removeFirst() {
        assert !isEmpty();
        T ret = arr[head];
        arr[head] = null;
        head = (head + 1) & (arr.length - 1);
        return ret;
    }
    
    public T removeLast() {
        assert !isEmpty();
        tail = (tail - 1) & (arr.length - 1);
        T ret = arr[tail]; 
        arr[tail] = null;
        return ret;
    }
    public T getLast() {
        assert !isEmpty();
        return arr[(tail - 1) & (arr.length - 1)];
    }
    @Override
    public String toString() {
        return "[MSNetArrayDequeue: head=" + head + " tail=" + tail + " arr=" + Arrays.toString(arr) + "]";
    }
}