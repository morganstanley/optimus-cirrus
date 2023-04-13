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

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.jgrapht.util.FibonacciHeap;
import org.jgrapht.util.FibonacciHeapNode;

public class FibPriorityQueue<T> extends AbstractQueue<T> {
  private FibonacciHeap<T> heap = new FibonacciHeap<>();

  @Override
  public boolean add(T e) {
    return false;
  }

  public boolean add(T e, Double k) {
    FibonacciHeapNode<T> n = new FibonacciHeapNode<>(e);
    heap.insert(n, k);
    return true;
  }

  @Override
  public boolean offer(T e) {
    return add(e);
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  private class FibPriorityQueueIterator<T> implements Iterator<T> {
    FibPriorityQueue<T> pq;

    public FibPriorityQueueIterator(FibPriorityQueue<T> givenPq) {
      this.pq = givenPq;
    }

    @Override
    public boolean hasNext() {
      return !this.pq.heap.isEmpty();
    }

    @Override
    public T next() {
      T nxt = this.pq.poll();
      if (nxt == null) {
        throw new NoSuchElementException();
      } else {
        return nxt;
      }
    }
  }

  @Override
  public Iterator<T> iterator() {
    return new FibPriorityQueueIterator<>(this);
  }

  @Override
  public int size() {
    return heap.size();
  }

  @Override
  public T poll() {
    if (heap.min() == null) {
      return null;
    } else {
      return heap.removeMin().getData();
    }
  }

  @Override
  public T peek() {
    if (heap.min() == null) {
      return null;
    } else {
      return heap.min().getData();
    }
  }
}
