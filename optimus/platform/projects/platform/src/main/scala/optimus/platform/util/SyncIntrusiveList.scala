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
package optimus.platform.util
import scala.annotation.tailrec

/**
 * A synchronized linked list of nodes with a Linked trait for nodes that should be mixed in.
 */
final class SyncIntrusiveList[T](lock: AnyRef) {
  def size: Int = lock.synchronized(_size)
  def total: Long = _total // can only go up, so no synchronization needed
  def foreach(f: T => Unit): Unit = {
    @tailrec def foreachImpl(current: Link)(f: T => Unit): Unit = {
      f(current)
      if (current.prev != null) foreachImpl(current.prev)(f)
    }
    lock.synchronized(foreachImpl(last)(f))
  }

  private type Link = T with Linked
  private var _total = 0L // all objects ever added
  private var _size = 0 // current size
  private var last: Link = _

  trait Linked { self: T =>
    def onRelease(): Unit // override to setup cleaning actions
    private[SyncIntrusiveList] var prev: Link = _
    private var next: Link = _

    // Append ourselves to the linked list so we don't get collected.  (Our referent of course might
    // be - that's the point of weaker ref)
    lock.synchronized {
      if (last eq null) // linked list was empty
        last = this // now it's not
      else {
        last.next = this // current last item now points to us
        this.prev = last // we point back to it
        last = this // and we become last
      }
      _total += 1
      _size += 1
    }

    def release(): Unit = {
      lock.synchronized {
        if (this.prev ne null) // Unless we were first in the list,
          this.prev.next = this.next // set previous item to skip over us.
        if (next eq null) // If we were at the end of this list,
          last = this.prev // then the new end is whatever was before us (which might be null).
        else
          this.next.prev = this.prev // Set the next item to skip over us.
        _size -= 1
      }
      onRelease()
    }
  }
}
