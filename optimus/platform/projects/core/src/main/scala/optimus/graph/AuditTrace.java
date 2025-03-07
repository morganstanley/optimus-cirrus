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
package optimus.graph;

import optimus.graph.cache.NCSupport;
import optimus.platform.EvaluationQueue;
import optimus.platform.storable.Entity;

public final class AuditTrace {
  private AuditTrace() {}

  private static AuditVisitor _visitor;

  public static boolean hasVisitor() {
    return _visitor != null;
  }

  public static void register(AuditVisitor visitor) {
    register(visitor, true);
  }

  static void register(AuditVisitor visitor, boolean checkEnabled) {
    if (checkEnabled && !Settings.auditing)
      throw new GraphException("Audit should be enabled by passing -Doptimus.audit=1");
    _visitor = visitor;
  }

  // For testing!
  public static void deregister() {
    _visitor = null;
  }

  /** Called by runtime */
  public static void visit(Entity e) {
    if (_visitor == null) return;

    try {
      OGSchedulerContext ec = OGSchedulerContext.current();
      if (ec != null) {
        NodeTask ntsk = ec.getCurrentNodeTask();
        if (ntsk != null) {
          boolean auditorEnabled = !ntsk.scenarioStack().auditorCallbacksDisabled();
          NodeExtendedInfo info = ntsk.info();
          if (auditorEnabled) {
            NodeExtendedInfo ninfo = _visitor.visit(e, info);
            if (ninfo != info) ec.getCurrentNodeTask().combineXinfo(ninfo);
          }
        } else {
          System.err.println("visit(Entry): current OGSchedulerContext has no node task.");
        }
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      // Better handle? Auditor has to observe and not throw
      System.err.println("visit(Entity): Auditor threw an exception\n" + ex.getMessage());
    }
  }

  static void visit(NodeTask ntsk) {
    if (_visitor == null || ntsk.scenarioStack().auditorCallbacksDisabled()) return;

    try {
      NodeExtendedInfo info = ntsk.info();
      NodeExtendedInfo ninfo = _visitor.visit(ntsk, info);
      if (ninfo != info) ntsk.setInfo(ninfo);
    } catch (Exception ex) {
      ex.printStackTrace();
      // Better handle? Auditor has to observe and not throw
      System.err.println("visit(NodeTask): Auditor threw an exception\n" + ex.getMessage());
    }
  }

  // return true if this node is getting rescheduled
  @SuppressWarnings("unchecked")
  static boolean visitBeforeRun(final NodeTask ntsk, OGSchedulerContext ec) {
    if (_visitor == null
        || ntsk.scenarioStack().auditorCallbacksDisabled()
        || NCSupport.isDelayedProxy(ntsk)) return false;

    final NodeExtendedInfo info = ntsk.info();
    final Node<NodeExtendedInfo> preVisitNode;
    try {
      Node<?> visitorNode = (Node<?>) _visitor.visitBeforeRun$newNode(ntsk, info);

      if (visitorNode == null) return false;

      if (!visitorNode.isDoneOrRunnable()) {
        throw new IllegalStateException("node should be either done or runnable: " + visitorNode);
      }

      // [SEE_AUDIT_TRACE_SI]
      var visitBeforeRunIsSI = visitorNode.executionInfo().isScenarioIndependent();
      var ss = visitBeforeRunIsSI ? ntsk.scenarioStack().asSiStack() : ntsk.scenarioStack();

      preVisitNode = (Node<NodeExtendedInfo>) visitorNode.prepareForExecutionIn(ss);
      preVisitNode.updateState(NodeTask.STATE_STARTED, 0);
      ec.enqueue(preVisitNode);

    } catch (Exception ex) {
      System.err.println(
          "visitBeforeRun: Auditor threw an exception\n"
              + ex.getMessage()); // Better handle? Auditor has to observe and not throw
      ex.printStackTrace();
      return false;
    }

    preVisitNode.continueWith(
        new NodeAwaiter() {
          @Override
          public void onChildCompleted(EvaluationQueue eq, NodeTask node) {
            if (preVisitNode.isDoneWithException()) {
              var e = preVisitNode.exception();
              var visitBeforeRunIsSI = preVisitNode.executionInfo().isScenarioIndependent();
              var toThrow = e;
              if (e instanceof IllegalScenarioDependenceException && !visitBeforeRunIsSI) {
                var exception = (IllegalScenarioDependenceException) e;
                // If visitBeforeRun is not itself SI, it can try to read tweaks even when auditing
                // an SI node.
                // Since preVisitNode inherits the stack of the node it is auditing, there's no way
                // to see tweaks in the
                // parent scenarios when auditing an SI node. So, although the auditor is not
                // technically doing anything
                // wrong (a non-SI visitor should be able to read tweaks) and the underlying audited
                // node is not doing
                // anything wrong, we still should crash here because the auditor will not be seeing
                // the tweaks it thinks it
                // is seeing. This wrapping of the exception message just makes this clearer to
                // users.
                toThrow =
                    new IllegalScenarioDependenceInAuditor(
                        exception.nonSInode(), preVisitNode, ntsk);
              }

              toThrow.printStackTrace();
              if (Settings.throwOnAuditorException) ntsk.completeWithException(toThrow, eq);
              else
                System.err.println(
                    "visitBeforeRun: Auditor threw an (preVisitNode) exception\n"
                        + toThrow
                            .getMessage()); // Better handle? Auditor has to observe and not throw
            } else {
              NodeExtendedInfo ninfo = preVisitNode.result();
              if (ninfo != info) ntsk.setInfo(ninfo);
            }

            if (!ntsk.isDoneOrRunnable()) ntsk.markAsRunnable();
            eq.enqueue(null, ntsk);
          }
        },
        ec);
    ntsk.setWaitingOn(preVisitNode);
    return true;
  }
}
