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
package optimus.dsi

import optimus.platform.dsi.bitemporal.LeadWriterCommand
import optimus.platform.dsi.bitemporal.PutApplicationEvent
import java.time.Instant
import optimus.dsi.base.immutableMultimapOf

package object timelord {

  /**
   * Inverts the TimelordSystems configuration to show you which types are written by which systems.
   */
  def writeTypesToSystems(timelordSystems: Set[TimelordSystem]): Map[String, List[String]] = {
    immutableMultimapOf((timelordSystems flatMap { case TimelordSystem(system, entitlements) =>
      entitlements collect { case TimelordEntitlement(_, useCase: Timelord.Mastering, writeEntityTypes) =>
        writeEntityTypes map { case tpe =>
          (tpe, system)
        }
      }
    }).flatten.toList)
  }

  /**
   * Inverts the TimelordSystems configuration into a map of which permissioned connections are performing which
   * usecase.
   */
  def connectionsToTypes(timelordSystems: Set[TimelordSystem]): Map[TimelordConnection, TimelordType] = {
    (timelordSystems flatMap { case TimelordSystem(_, entitlements) =>
      entitlements collect { case TimelordEntitlement(connection, useCase, _) =>
        connection -> useCase
      }
    }).toMap
  }

  /**
   * Extracts all the types in a PutApplicationEvent request
   */
  def primaryTypesOfReq(cmds: Seq[LeadWriterCommand]): Seq[String] = {
    val b = cmds flatMap {
      case PutApplicationEvent(bes, _, _, Some(externalTxTime), _, _, _, _, _) =>
        Some(bes)
      case _ =>
        None
    }

    b.flatten flatMap { case bb =>
      bb.puts.map(_.ent.className) ++ bb.puts.flatMap(_.ent.types) ++ Seq(bb.evt.className) ++ bb.evt.types
    }
  }

  /**
   * Flips the fencepost history of types written by system to be
   */
  def typeIdsToSystems(fenceposts: Map[String, (Instant, Seq[String])]): Map[String, List[String]] = {
    val flipped = fenceposts.toList flatMap { case (system, (_, tids)) =>
      tids map { tid =>
        (tid, system)
      }
    }
    immutableMultimapOf(flipped)
  }

  /**
   * Creates a map of which systems effect which types
   */
  def systemTypes(timelordSystems: Set[TimelordSystem]): Map[String, Set[String]] = {
    (timelordSystems flatMap { case TimelordSystem(system, entitlements) =>
      entitlements flatMap {
        case TimelordEntitlement(_, cu: Timelord.Mastering, writeEntityTypes) =>
          Some(system -> writeEntityTypes)
        case _ =>
          None
      }
    }).toMap
  }
}
