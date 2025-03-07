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
package optimus.graph.loom.compiler;

/**
 * Compiler phase. Given a DAG of blocks, it tries to "reduce" blocks by merging multiple blocks
 * together. This allows us to schedule node codes earlier.
 */
class BlockFolding {

  private final DCFlowGraph state;

  public BlockFolding(DCFlowGraph state) {
    this.state = state;
  }

  void reduce() {
    for (int i = state.blocks.size() - 1; i >= 0; i--) {
      reduce(state.blocks.get(i));
    }
  }

  private void reduce(Block block) {
    if (state.compilerArgs.skipFoldingBlocks) return;
    if (block.targets != null && block.targets.length == 2) {
      var t0 = block.targets[0];
      var t1 = block.targets[1];
      if (t0.hasSameSingleTargetAndPredecessor(t1)) {
        var commonTarget = t0.targets[0];
        // we cannot fold in commonTarget if it has other predecessors other than t0 and t1!
        if (commonTarget.predecessors.size() == 2) {
          var branchOp = (Op.Branch) block.lastOp;
          reroutePhiFunc(t0, t1, commonTarget, branchOp);

          for (var phiOp : commonTarget.phiOps) {
            commonTarget.availOps.remove(phiOp);
            branchOp.addConsumer(phiOp);
          }

          // Inline/Remove dependencies that went to the block being inlined
          for (var readVar : commonTarget.readsVars) {
            var storeOp = readVar.singleInput();
            if (block.matches(storeOp.blockOwner)) {
              storeOp.removeDependency(branchOp); // [SEE_BLOCK_VAR_DEPENDENCY]
            } else block.addReadVar(readVar);
            if (readVar.removeIncomingEdge(commonTarget) == 0) {
              block.availOps.add(readVar);
            }
          }

          t0.readVarInputsAsDependency(branchOp);
          t1.readVarInputsAsDependency(branchOp);

          branchOp.commonTarget = commonTarget;
          block.lastOp = commonTarget.lastOp;
          block.availOps.addAll(commonTarget.availOps);
          commonTarget.foldedIn = true;
          block.foldedBlocks.addAll(commonTarget.foldedBlocks);
          block.foldedBlocks.add(commonTarget);
          for (var foldedBlock : block.foldedBlocks)
            foldedBlock.blockID = block.blockID; // Renamed ALL the ops
          block.targets = commonTarget.targets;
          commonTarget.updatePhiTypes(state);
        }
      }
    }
  }

  private void reroutePhiFunc(Block t0, Block t1, Block commonTarget, Op.Branch branchOp) {
    LMessage.require(t0.phiOps.size() == t1.phiOps.size(), "Same phiOps size expected");
    int stackCount = t0.phiOps.size(); // originalStackCount
    for (int i = 0; i < stackCount; i++) {
      var phi0 = t0.phiOps.get(i);
      var phi1 = t1.phiOps.get(i);
      if (phi0.stackSlot >= 0 && phi1.stackSlot >= 0) {
        var sourceOp = phi0.singleInput();
        LMessage.require(sourceOp == phi1.singleInput(), "Same single input expected");

        var targetPhi = phi0.singleConsumer();
        LMessage.require(targetPhi == phi1.singleConsumer(), "Same single consumer expected");

        var targetOp = targetPhi.singleConsumer();
        sourceOp.removeConsumer(phi0);
        sourceOp.removeConsumer(phi1);
        sourceOp.consumers.add(targetOp);
        targetOp.changeInputFromTo(targetPhi, sourceOp);

        t0.availOps.remove(phi0);
        t1.availOps.remove(phi1);
        commonTarget.availOps.remove(targetPhi);
        //noinspection SuspiciousMethodCalls ...we know that targetPhi is of type Op.Phi!
        commonTarget.phiOps.remove(targetPhi);

        // no longer delaying ops between blocks if we merge these blocks!
        if (branchOp.inputs.contains(sourceOp)) {
          sourceOp.removeConsumer(branchOp);
          branchOp.removeInput(sourceOp);
        }
      }
    }
  }
}
