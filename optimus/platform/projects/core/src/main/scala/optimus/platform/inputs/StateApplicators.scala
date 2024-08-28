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
package optimus.platform.inputs

import optimus.platform.inputs.loaders.ProcessSINodeInputMap

object StateApplicators {
  private def getInputFromStateOrCrash[T](
      processSINodeInputMap: ProcessSINodeInputMap,
      nodeInput: ProcessSINodeInput[T]): T =
    processSINodeInputMap.getInput(nodeInput) match {
      case Some(v) => v
      case None    => throw new IllegalStateException("Must provide all dependent inputs!")
    }

  /**
   * Base trait that defines how to set/reset the state of a given [[ProcessSINodeInput]]
   * @apiNote
   *   this class should not be extended when creating [[ProcessSINodeInput]], depending on how many dependencies it has
   *   the StateApplicatorX class should be used
   */
  private[inputs] trait StateApplicator {

    /**
     * All of the inputs that a given [[ProcessSINodeInput]] depends on to set/reset. For example stall detector also
     * timing intervals, detectAdaptedStalls, etc
     * @apiNote
     *   a node input always depends on itself
     */
    def dependencies: Seq[ProcessSINodeInput[_]]

    /**
     * @param firstTime: some applicators will have different behavior if it is the first time that is being run (e.g. for XSFT first time nothing needs to happen cause the value will just be read in NTI constructor but next time it needs to change all the NTIs to what is specified)
     */
    def apply(processSINodeInputMap: ProcessSINodeInputMap, firstTime: Boolean): Unit

    private[inputs] def resetState(): Unit
  }

  val emptyApplicator: StateApplicator = new StateApplicator {
    override def dependencies: Seq[ProcessSINodeInput[_]] = Seq.empty
    override def apply(processSINodeInputMap: ProcessSINodeInputMap, firstTime: Boolean): Unit = ()
    override private[inputs] def resetState(): Unit = ()
  }

  trait StateApplicator1[T] extends StateApplicator {
    override final def dependencies: Seq[ProcessSINodeInput[T]] = List(nodeInput)

    def nodeInput: ProcessSINodeInput[T]

    override final def apply(processSINodeInputMap: ProcessSINodeInputMap, firstTime: Boolean): Unit = {
      apply(getInputFromStateOrCrash[T](processSINodeInputMap, nodeInput), firstTime)
    }

    private[inputs] def apply(value: T, firstTime: Boolean): Unit
  }

  trait StateApplicator2[T1, T2] extends StateApplicator {
    override final def dependencies: List[ProcessSINodeInput[_]] = List(nodeInputs._1, nodeInputs._2)

    def nodeInputs: (ProcessSINodeInput[T1], ProcessSINodeInput[T2])

    override final def apply(processSINodeInputMap: ProcessSINodeInputMap, firstTime: Boolean): Unit = {
      apply(
        getInputFromStateOrCrash(processSINodeInputMap, nodeInputs._1),
        getInputFromStateOrCrash(processSINodeInputMap, nodeInputs._2),
        firstTime)
    }

    private[inputs] def apply(value1: T1, value2: T2, firstTime: Boolean): Unit
  }

  trait StateApplicator3[T1, T2, T3] extends StateApplicator {
    override final def dependencies: List[ProcessSINodeInput[_]] = List(nodeInputs._1, nodeInputs._2, nodeInputs._3)

    def nodeInputs: (ProcessSINodeInput[T1], ProcessSINodeInput[T2], ProcessSINodeInput[T3])

    override final def apply(processSINodeInputMap: ProcessSINodeInputMap, firstTime: Boolean): Unit = {
      apply(
        getInputFromStateOrCrash(processSINodeInputMap, nodeInputs._1),
        getInputFromStateOrCrash(processSINodeInputMap, nodeInputs._2),
        getInputFromStateOrCrash(processSINodeInputMap, nodeInputs._3),
        firstTime
      )
    }

    private[inputs] def apply(value1: T1, value2: T2, value3: T3, firstTime: Boolean): Unit
  }

  trait StateApplicator4[T1, T2, T3, T4] extends StateApplicator {
    override final def dependencies: List[ProcessSINodeInput[_]] =
      List(nodeInputs._1, nodeInputs._2, nodeInputs._3, nodeInputs._4)

    def nodeInputs: (ProcessSINodeInput[T1], ProcessSINodeInput[T2], ProcessSINodeInput[T3], ProcessSINodeInput[T4])

    override final def apply(processSINodeInputMap: ProcessSINodeInputMap, firstTime: Boolean): Unit = {
      apply(
        getInputFromStateOrCrash(processSINodeInputMap, nodeInputs._1),
        getInputFromStateOrCrash(processSINodeInputMap, nodeInputs._2),
        getInputFromStateOrCrash(processSINodeInputMap, nodeInputs._3),
        getInputFromStateOrCrash(processSINodeInputMap, nodeInputs._4),
        firstTime
      )
    }

    private[inputs] def apply(value1: T1, value2: T2, value3: T3, value4: T4, firstTime: Boolean): Unit
  }
}
