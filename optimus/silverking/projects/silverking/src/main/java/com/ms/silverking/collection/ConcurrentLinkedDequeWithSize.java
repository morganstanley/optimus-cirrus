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
package com.ms.silverking.collection;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A subset of ConcurrentLinkedDeque functionality with a constant time size function.
 * Note that size is only loosely consistent with the actual size at any given moment.
 * <p>
 * Exceptions in the underlying deque may cause the reported size to diverge from reality.
 * This class is intended for the case where such as exception will either not occur, or be fatal.
 */
public class ConcurrentLinkedDequeWithSize<E> {
  private final ConcurrentLinkedDeque<E> dq;
  private final AtomicInteger size;

  public ConcurrentLinkedDequeWithSize() {
    dq = new ConcurrentLinkedDeque();
    size = new AtomicInteger();
  }

  public E poll() {
    E e;

    e = dq.poll();
    if (e != null) {
      size.decrementAndGet();
    }
    return e;
  }

  public E remove() {
    size.decrementAndGet();
    return dq.remove();
  }

  public E peek() {
    return dq.peek();
  }

  public void push(E e) {
    size.incrementAndGet();
    dq.push(e);
  }

  public boolean isEmpty() {
    return dq.isEmpty();
  }

  public boolean add(E e) {
    size.incrementAndGet();
    return dq.add(e);
  }

  public int size() {
    return size.get();
  }
}
