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
package optimus.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import optimus.graph.DiagnosticSettings;
import optimus.graph.diagnostics.PNodeTask;

@SuppressWarnings("StaticInitializerReferencesSubClass")
public abstract class EdgeList extends ArrayList<PNodeTask> {
  public static final EdgeList empty = new EdgeListWithEnqueues();

  public static EdgeList newDefault() {
    return new EdgeListWithEnqueues();
  }

  public static EdgeList newSeqOps(int maxConcurrency) {
    return new EdgeListSequence(maxConcurrency);
  }

  /** Only return 'result collect edges' */
  public abstract Iterator<PNodeTask> withoutEnqueues();
  /**
   * Return enqueued edges that are not 'done' and thefore will not be repeated in 'result collect
   * edges'
   */
  public abstract Iterator<PNodeTask> withNotDoneEnqueues();

  public abstract void addEnqueueEdge(PNodeTask child);

  public abstract boolean isEnqueue(int index);

  public abstract boolean hasEnqueues();

  public abstract int sizeWithoutEnqueues();
}

class EdgeListWithEnqueues extends EdgeList {
  private boolean[] enqueued;

  public Iterator<PNodeTask> withoutEnqueues() {
    if (enqueued == null) return super.iterator();
    return new Itr(true);
  }

  public Iterator<PNodeTask> withNotDoneEnqueues() {
    if (enqueued == null) return super.iterator();
    return new Itr(false);
  }

  private class Itr implements Iterator<PNodeTask> {
    boolean skipAllEnqueues; // As opposed only
    int cursor; // index of next element to return

    public Itr(boolean keepNonDoneEnqueues) {
      this.skipAllEnqueues = keepNonDoneEnqueues;
    }

    private void skipEnqueues() {
      while (isEnqueue(cursor)) {
        if (skipAllEnqueues || get(cursor).isDone()) cursor++;
        else break;
      }
    }

    public boolean hasNext() {
      skipEnqueues();
      return cursor != size();
    }

    public PNodeTask next() {
      skipEnqueues();
      return get(cursor++);
    }
  }

  @Override
  public void addEnqueueEdge(PNodeTask child) {
    int index = size();
    super.add(child);
    if (enqueued == null) enqueued = new boolean[index + 1];
    else if (index + 1 >= enqueued.length) enqueued = Arrays.copyOf(enqueued, index * 2);

    enqueued[index] = true;
  }

  /** enqueue (forward) edge, not dependency (back) edge */
  @Override
  public boolean isEnqueue(int index) {
    return enqueued != null && index < enqueued.length && enqueued[index];
  }

  @Override
  public int sizeWithoutEnqueues() {
    if (enqueued == null) return size();
    int i = 0;
    int size = size();
    while (i < enqueued.length) {
      if (enqueued[i]) size--;
      i++;
    }
    return size;
  }

  @Override
  public boolean hasEnqueues() {
    return enqueued != null;
  }
}

/**
 * Customized edge list for sequences Sequences have lazy expansion logic (like a bootstrap
 * iterator) therefore just observing the calls to enqueue and combineInfo/collect will not
 * represent the correct picture to the concurrency analyzer
 *
 * <p>There is also no point of keeping both enqueue and collect edges Therefore: depending on the
 * showEnqueuedNotCompletedNodes flag we will keep just one type or the other
 *
 * <p>Note: in the case of showEnqueuedNotCompletedNodes == true and some iterations are ignored or
 * aborted via cancellation scopes for example, this code currently will show the edges from
 * uncollected iterations. Hopefully one would not care ...
 *
 * <p>Currently only maxConcurrency = 1 and maxConcurrency = MAX (infinite) are supported [enqueue]
 * { concurrency } [collect enqueue] {N - concurrency} [collect] { concurrency }
 */
class EdgeListSequence extends EdgeList {
  private final int maxConcurrency;

  EdgeListSequence(int maxConcurrency) {
    this.maxConcurrency = maxConcurrency;
  }

  @Override
  public PNodeTask get(int index) {
    int size = super.size();
    if (maxConcurrency == 1) return super.get(index / 2);
    else if (maxConcurrency >= size)
      return index < size ? super.get(index) : super.get(index - size);
    // TODO (OPTIMUS-43368): support maxConcurrency, ie throttle
    else throw new IllegalArgumentException();
  }

  @Override
  public int size() {
    return super.size() * 2; // We pretend we have all the enqueue edges matching collect edges
  }

  @Override
  public int sizeWithoutEnqueues() {
    return super.size();
  }

  @Override
  public Iterator<PNodeTask> withoutEnqueues() {
    return super.iterator();
  }

  @Override
  public Iterator<PNodeTask> withNotDoneEnqueues() {
    return super.iterator();
  }

  @Override
  public boolean add(PNodeTask pNodeTask) {
    if (!DiagnosticSettings.showEnqueuedNotCompletedNodes) super.add(pNodeTask);
    // else ignore on purpose [SEE_showEnqueuedNotCompletedNodes]
    return true; // Required by interface of Collection.add
  }

  @Override
  public void addEnqueueEdge(PNodeTask child) {
    if (DiagnosticSettings.showEnqueuedNotCompletedNodes) super.add(child);
    // else ignore on purpose [SEE_showEnqueuedNotCompletedNodes]
  }

  @Override
  public boolean hasEnqueues() {
    return true;
  }

  @Override
  public boolean isEnqueue(int index) {
    int size = super.size();
    if (maxConcurrency == 1) return index % 2 == 0;
    else if (maxConcurrency >= size)
      // assume not interleaved for EdgeListSequence (all enqueue edges come before all collects)
      return index < size;
    // TODO (OPTIMUS-43368): support maxConcurrency, ie throttle
    else throw new IllegalArgumentException();
  }
}
