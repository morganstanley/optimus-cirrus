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
import optimus.entity.IndexInfo
import optimus.entity.ModuleEntityInfo

import java.time.Instant

package object storable {

  implicit class entityInternals(val e: Entity) extends AnyVal {

    def $isModule: Boolean = e.$info.isInstanceOf[ModuleEntityInfo]

    def coreGetProps(): String = new EntityPrettyPrintView(e).dumpProps
    def coreGetPropsAsSeq: Seq[String] = new EntityPrettyPrintView(e).propsSeq
    def corePrettyPrint: String = new EntityPrettyPrintView(e).prettyPrint

    def allKeyInfos[E <: Entity]: Seq[IndexInfo[E, _]] = e.$info.keys.map(_.asInstanceOf[IndexInfo[E, _]])

    final private def defaultKeyInfoOpt[E <: Entity]: Option[IndexInfo[E, _]] = {
      val keys = allKeyInfos[E]
      keys.find(_.default).orElse(keys.headOption)
    }

    final private def defaultKeyString: Option[String] = defaultKeyInfoOpt[Entity].map { key =>
      val keyToString =
        try { key.entityToString(e) }
        catch {
          case ex: Exception =>
            e.log.debug(s"Cant generate toString", ex)
            "<cant generate - exception logged>"
        }
      s"${key.name}=($keyToString)"
    }

    private[optimus] final def entityInfoString: String =
      defaultKeyString
        .map { keyString =>
          val name = {
            val raw = e.getClass.getName
            raw.substring(raw.lastIndexOf('.') + 1)
          }

          s"$name($keyString)"
        }
        .getOrElse {
          if (e.$info.isInstanceOf[ModuleEntityInfo]) {
            // for entity object it doesn't make sence to include the hashcode
            // we also have test that assume that the toString of some entity objects is stable :-(
            val raw = e.getClass.getName
            raw.substring(raw.lastIndexOf('.') + 1)
          } else e.getClass.getName + "@" + Integer.toHexString(hashCode())
        }

    def cmid: Option[MSUuid] = e.entityFlavorInternal.dal$cmid
    private[optimus] def dal$cmid_=(cmid: Option[MSUuid]): Unit = EntityInternals.prepareMutate(e).dal$cmid = cmid

    // NB: These are public because they are accessed via code injected into user packages.
    final def dal$entityRef_=(entityRef: EntityReference): Unit = EntityInternals.prepareMutate(e).dal$entityRef =
      entityRef

    private[optimus] final def dal$storageInfo_=(info: StorageInfo): Unit = EntityInternals.setDal$storageInfo(e, info)

    // back door to allow DAL update of storage info.
    private[optimus] final def dal$storageInfoUpdate(info: StorageInfo): Unit =
      EntityInternals.prepareMutate(e).dal$storageInfo = info

    private[optimus] final def dal$inlineEntities: InlineEntityHolder =
      e.entityFlavorInternal.dal$inlineEntities
    final def dal$isTemporary: Boolean = e.entityFlavorInternal.dal$isTemporary
    private[optimus] final def dal$universe: EntityUniverse = e.entityFlavorInternal.universe

    // Private to optimus while we decide how to refactor
    private[optimus] final def txTime: Instant = e.dal$storageInfo.txTime

    // Private to optimus while we decide how to refactor
    private[optimus] final def validTime: ValidTimeInterval = e.dal$storageInfo.validTime

    private[optimus] def isTxValidTime: Boolean = e.validTime.from == e.txTime
  }
}
