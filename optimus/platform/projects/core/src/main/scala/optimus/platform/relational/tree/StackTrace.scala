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
package optimus.platform.relational.tree

final case class StackTrace(msg: String, elems: Seq[StackTraceElement]) {
  override def toString() = msg + "\n" + elems.map("        at " + _).mkString("\n")
}

object StackTrace {
  import Packer._
  implicit val stackTraceElementPacker: Packer[StackTraceElement] = new Packer[StackTraceElement] {
    def serialize(ste: StackTraceElement, state: Packer.WriteState): Unit = {
      implicitly[Packer[Seq[String]]].serialize(ste.getClassName.split("\\."), state)
      implicitly[Packer[String]].serialize(ste.getMethodName, state)
      implicitly[Packer[Seq[String]]].serialize(Option(ste.getFileName).toSeq.flatMap(_.split("\\.")), state)
      implicitly[Packer[Int]].serialize(ste.getLineNumber + 2, state)
    }
  }

  implicit val stackTraceElementUnpacker: Unpacker[StackTraceElement] = new Unpacker[StackTraceElement] {
    def deserialize(state: Unpacker.ReadState): StackTraceElement = {
      new StackTraceElement(
        implicitly[Unpacker[Seq[String]]].deserialize(state) mkString ".",
        implicitly[Unpacker[String]].deserialize(state),
        implicitly[Unpacker[Seq[String]]].deserialize(state) mkString ".",
        implicitly[Unpacker[Int]].deserialize(state) - 2
      )
    }
  }

  implicit val stackTracePacker: Packer[StackTrace] = new Packer[StackTrace] {
    def serialize(st: StackTrace, state: Packer.WriteState): Unit = {
      implicitly[Packer[String]].serialize(st.msg, state)
      implicitly[Packer[Seq[StackTraceElement]]].serialize(st.elems, state)
    }
  }

  implicit val stackTraceUnpacker: Unpacker[StackTrace] = new Unpacker[StackTrace] {
    def deserialize(state: Unpacker.ReadState): StackTrace = {
      StackTrace(
        implicitly[Unpacker[String]].deserialize(state),
        implicitly[Unpacker[Seq[StackTraceElement]]].deserialize(state))
    }
  }
}
