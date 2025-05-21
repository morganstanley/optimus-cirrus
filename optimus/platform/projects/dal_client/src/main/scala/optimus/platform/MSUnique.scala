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
package optimus.platform

import msjava.base.util.uuid.MSUuid
import optimus.platform._

sealed trait MSUnique {
  def getSavedUuid: MSUuid
}

object MSUnique {
  // The value should return a MSUnique instance which is different from others
  def value: MSUnique = new TransientMSUnique()

  // this is give the user a way to create MSUnique with exist uuid
  def apply(uuid: MSUuid): MSUnique = new PersistentMSUnique(uuid)

  // This is added to support serialization in Java.
  def getUuid = value.getSavedUuid

  private class TransientMSUnique extends MSUnique {
    private[MSUnique] var savedUuid: MSUuid = null
    // This is added to support serialization in Java.
    def getSavedUuid = savedUuid
  }

  private final case class PersistentMSUnique(private val uuid: MSUuid) extends MSUnique {
    override def hashCode = uuid.hashCode
    // This is added to support serialization in Java.
    def getSavedUuid = uuid

    override def equals(o: Any): Boolean = o match {
      // Cannot equal a transient MSUnique
      case other: PersistentMSUnique => (this eq other) || (this.uuid == other.uuid)
      case _                         => false
    }
  }

  import optimus.platform.pickling._
  import DefaultPicklers._
  import DefaultUnpicklers._

  object MSUniquePickler extends Pickler[MSUnique] {
    override def pickle(unique: MSUnique, visitor: PickledOutputStream) = unique match {
      case PersistentMSUnique(uuid) =>
        Registry.picklerOf[MSUuid].pickle(uuid, visitor)
      case t: TransientMSUnique =>
        // here we will generate the UUID and save it into both the instance and output stream
        if (t.savedUuid == null)
          t.savedUuid = new MSUuid // generated 23 byte long UUID
        Registry.picklerOf[MSUuid].pickle(t.savedUuid, visitor)
    }
  }

  object MSUniqueUnpickler extends Unpickler[MSUnique] {
    @node def unpickle(pickled: Any, ctxt: PickledInputStream): MSUnique = pickled match {
      case buf: collection.Seq[_] =>
        PersistentMSUnique(Registry.unpicklerOf[MSUuid].unpickle(pickled, ctxt))
    }
  }
}

// This is added to support serialization in Java.
object MSUniqueBuilder {
  def build(uuid: MSUuid) = MSUnique(uuid)
  def value = MSUnique.value
}
