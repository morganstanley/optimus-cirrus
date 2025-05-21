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

package msjava.zkapi.internal;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;
public final class AtomicLatch {
    @Override
    public int hashCode() {
        return latch.get().hashCode();
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AtomicLatch other = (AtomicLatch) obj;
        return latch.get().equals(other.latch.get());
    }
    @Nonnull
    private final AtomicReference<CountDownLatch> latch;
    public AtomicLatch() {
        latch = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
    }
    public AtomicLatch(AtomicLatch other) {
        latch = new AtomicReference<CountDownLatch>(other.latch.get());
    }
    private AtomicLatch(AtomicLatch other, CountDownLatch newLatch) {
        latch = new AtomicReference<CountDownLatch>(other.latch.getAndSet(newLatch));
    }
    public void fire() {
        latch.get().countDown();
    }
    public void replace() {
        latch.set(new CountDownLatch(1));
    }
    
    public void replaceWith(AtomicLatch other) {
        latch.set(other.latch.get());
    }
    
    public AtomicLatch getAndReplace() {
        return new AtomicLatch(this, new CountDownLatch(1));
    }
    public void fireAndReplace() {
        latch.getAndSet(new CountDownLatch(1)).countDown();
    }
    
    public boolean fireAndReplace(AtomicLatch other) {
        CountDownLatch l = latch.getAndSet(new CountDownLatch(1));
        l.countDown();
        return l.equals(other.latch.get());
    }
    public boolean isFired() {
        return latch.get().getCount() == 0;
    }
    public boolean await() throws InterruptedException {
        return await(0);
    }
    
    public boolean await(long timeout) throws InterruptedException {
        CountDownLatch latch = this.latch.get();
        if(timeout == 0) {
            latch.await();
            return true;
        }
        return latch.await(timeout, TimeUnit.MILLISECONDS);
    }
}
