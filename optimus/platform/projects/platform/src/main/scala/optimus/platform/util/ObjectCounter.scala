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
import java.lang.ref.ReferenceQueue
import java.lang.ref.PhantomReference
import scala.annotation.tailrec

trait ObjectCounter[T] { counter =>
  private val list = new SyncIntrusiveList[Ref](counter)
  private val rq = new ReferenceQueue[T]
  def onRelease(): Unit = () // override to hook releases
  private class Ref(referent: T) extends PhantomReference(referent, rq) with list.Linked {
    override def onRelease(): Unit = counter.onRelease()
  }
  def register(r: T): T = {
    new Ref(r) // called for side effects
    cleanup()
    r
  }
  def total: Long = list.total
  def size: Int = list.size
  @tailrec final def cleanup(): Unit = {
    rq.poll() match {
      case null => ()
      case r: Ref =>
        r.release()
        cleanup()
      case r => throw new MatchError(r)
    }
  }
}
