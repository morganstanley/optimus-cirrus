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
package optimus.graph

trait Warning {
  def message: String
  override def toString = message
}

object Warning {
  def apply(message: String): Warning = new MessageAsWarning(message)
  def apply(exception: Throwable): Warning = new ExceptionAsWarning(exception)
}

object Note {
  def apply(message: String) = new MessageAsNote(message)
}

trait Note extends Warning

class MessageAsNote(val message: String) extends Note with Serializable {
  override def hashCode = message.hashCode
  override def equals(o: Any) = o match {
    case wm: MessageAsNote => wm.message == message
    case _                 => false
  }
}

class ExceptionAsWarning(val exception: Throwable) extends Warning with Serializable {
  override def message = exception.getMessage
  override def hashCode = message.hashCode ^ exception.getClass.hashCode
  override def equals(o: Any) = o match {
    case eaw: ExceptionAsWarning => (eaw.exception.getClass == exception.getClass) && (eaw.message == message)
    case _                       => false
  }
}

class MessageAsWarning(val message: String) extends Warning with Serializable {
  override def hashCode = message.hashCode
  override def equals(o: Any) = o match {
    case wm: MessageAsWarning => wm.message == message
    case _                    => false
  }
}
