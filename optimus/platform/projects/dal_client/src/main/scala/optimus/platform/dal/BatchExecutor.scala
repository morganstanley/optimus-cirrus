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
package optimus.platform.dal

import java.time.Instant
import optimus.core.CoreAPI
import optimus.graph.DiagnosticSettings
import optimus.platform._
import optimus.platform.AsyncImplicits._
import optimus.platform.util.Log

import scala.util.control.NonFatal
import scala.util.{Success, Try}

object BatchExecutor extends Log {
  private[dal] val FallbackBatchSize = 100
  val BatchSizeProperty = "optimus.dsi.transaction.batchSize"
  def defaultBatchSize = DiagnosticSettings.getIntProperty(BatchSizeProperty, FallbackBatchSize)
}

class BatchExecutor(resolver: EntityResolverWriteOps, customBatchSize: Option[Int] = None) extends Log {
  import BatchExecutor._
  type Batch = List[Transaction]

  val batchSize = customBatchSize getOrElse defaultBatchSize

  @async def batchExecute(executables: Seq[BatchExecutable]): Map[Transaction, Try[PersistResult]] = {
    val transactions = executables.flatMap {
      case txn: Transaction         => List(txn)
      case seq: TransactionSequence => seq.transactions
    }
    require(!transactions.isEmpty, "This API must be called with at least one transaction")
    val groups = transactions.foldLeft[(List[Batch], Batch, Int)]((Nil, Nil, 0)) {
      case ((batchList, currentBatch, cnt), txn) =>
        if (
          currentBatch.isEmpty ||
          (cnt + txn.writeMass <= batchSize && noCollidingTxTimes(txn.clientTxTime, currentBatch.head.clientTxTime))
        )
          (batchList, txn :: currentBatch, cnt + txn.writeMass)
        else
          (currentBatch.reverse :: batchList, List(txn), txn.writeMass)
    }
    val batches = (groups._2.reverse :: groups._1).reverse

    val batchResults = batches.aseq.map { batch =>
      execute(batch)
    }
    val txnResults = (batches.zip(batchResults)).flatMap { case (batch, results) =>
      batch.zip(results)
    }
    txnResults.toMap
  }

  @async private[this] def execute(batch: Batch): List[Try[PersistResult]] = {
    val res = CoreAPI
      .asyncResult {
        val result = executeTogether(batch)
        val success = Success(result)
        batch map { _ =>
          success
        }
      }
      .recover {
        case NonFatal(ex) =>
          log.warn(
            s"Executing batch [$batch] failed with exception ${ex.getClass.getName}: ${ex.getMessage}. Will retry transactions one by one.",
            ex)
          executeOneByOne(batch)
        case ex =>
          log.error(s"Fatal error/exception caught", ex)
          throw ex
      }
    batch foreach (_.dispose())
    res.value
  }

  @async private[this] def executeTogether(batch: Batch): PersistResult = {
    val commands = batch.aseq.map { txn =>
      txn.createCommand()
    }
    resolver.executeAppEvent(
      commands.map(_.pae),
      commands.flatMap(_.cmdToEntityMap).toMap,
      mutateEntities = false,
      retryWrite = true,
      { _ =>
        null
      })
  }

  @async private[this] def executeOneByOne(batch: Batch): List[Try[PersistResult]] = batch.aseq.map { txn =>
    CoreAPI.asyncResult {
      val cmd = txn.createCommand()
      resolver.executeAppEvent(
        cmd.pae :: Nil,
        cmd.cmdToEntityMap,
        mutateEntities = false,
        retryWrite = true,
        { _ =>
          null
        })
    }.toTry
  }

  def noCollidingTxTimes(tt1: Option[Instant], tt2: Option[Instant]): Boolean = (tt1, tt2) match {
    case (None, None)                             => true
    case (x @ Some(tx), y @ Some(ty)) if tx == ty => true
    case _                                        => false
  }

}
