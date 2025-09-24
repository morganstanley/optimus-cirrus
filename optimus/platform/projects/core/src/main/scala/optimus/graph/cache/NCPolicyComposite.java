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
package optimus.graph.cache;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import optimus.graph.GraphInInvalidState;
import optimus.graph.PropertyNode;

/** Base class for common case of 1, 2, 3 policies */
public abstract class NCPolicyComposite extends NCPolicy {
  static final ConcurrentHashMap<NCPolicy, NCPolicy> policyCache = new ConcurrentHashMap<>();
  final NCPolicy defaultPolicy;

  NCPolicyComposite(NCPolicy defaultPolicy) {
    super(false, false, 0);
    this.defaultPolicy = defaultPolicy;
  }

  public NCPolicy defaultPolicy() {
    return this.defaultPolicy;
  }

  public final NCPolicy[] compositePolicies() {
    var composite = toCompositeN();
    return Arrays.stream(composite.policies).map(a -> a.policy).toArray(NCPolicy[]::new);
  }

  abstract CompositeN toCompositeN();

  @Override
  public NCPolicy pathIndependent() {
    return defaultPolicy;
  }

  @Override
  public NCPolicy withPathIndependentIfNotSet(NCPolicy scopeIndependent) {
    throw new GraphInInvalidState();
  }

  @Override
  public NCPolicy combineWith(String currentTargetPath, NCPolicy policy, String targetPath) {
    throw new GraphInInvalidState();
  }

  static class PolicyMask {
    static Comparator<PolicyMask> COMPARATOR =
        Comparator.comparingInt((PolicyMask pm) -> Long.bitCount(pm.mask))
            .thenComparing(pm -> pm.policy.getClass().getName());

    final NCPolicy policy;
    final long mask;

    PolicyMask(NCPolicy policy, long mask) {
      this.policy = policy;
      this.mask = mask;
    }
  }

  static class CompositeN extends NCPolicyComposite {
    final PolicyMask[] policies;

    CompositeN(NCPolicy defaultPolicy, NCPolicy policy, long mask) {
      this(defaultPolicy, new PolicyMask[] {new PolicyMask(policy, mask)});
    }

    CompositeN(NCPolicy defaultPolicy, PolicyMask[] policies) {
      super(defaultPolicy);
      for (var policy : policies)
        if (policy.policy instanceof NCPolicyComposite)
          throw new GraphInInvalidState("Composite policies cannot be nested");
      this.policies = policies;
    }

    /**
     * Creates a new composite policy with the given policy added to the list If the same policy was
     * already listed the mask is updated in the new policy only
     */
    @Override
    public NCPolicy combineWith(String currentTargetPath, NCPolicy policy, String targetPath) {
      // targetPath is empty means the default policy was provided
      if (targetPath.isEmpty()) return new CompositeN(policy, policies);

      var additionalMask = scopeMaskFromPath(targetPath);
      // Check if already such policy is present and we can just add the mask
      for (int i = 0; i < policies.length; i++) {
        var pol = policies[i];
        if (pol.policy == policy) {
          var newPolicies = Arrays.copyOf(policies, policies.length);
          newPolicies[i] = new PolicyMask(pol.policy, pol.mask | additionalMask);
          return new CompositeN(defaultPolicy, newPolicies);
        }
      }
      int policyCount = policies.length;
      var newPolicies = Arrays.copyOf(policies, policyCount + 1);
      newPolicies[policyCount] = new PolicyMask(policy, additionalMask);
      return new CompositeN(defaultPolicy, newPolicies);
    }

    /** Finalizes the policy with the pathIndependent policy if it wasn't set before */
    @Override
    public NCPolicy withPathIndependentIfNotSet(NCPolicy scopeIndependent) {
      return defaultPolicy == null
          ? compress(scopeIndependent, policies)
          : compress(defaultPolicy, policies);
    }

    @Override
    CompositeN toCompositeN() {
      return this;
    }

    @Override
    public NCPolicy downgradeToBasic() {
      var newDefault = defaultPolicy.downgradeToBasic();
      var newPolicies = new PolicyMask[policies.length];
      for (int i = 0; i < policies.length; i++) {
        var pm = policies[i];
        newPolicies[i] = new NCPolicyComposite.PolicyMask(pm.policy.downgradeToBasic(), pm.mask);
      }
      return compress(newDefault, newPolicies);
    }

    private static NCPolicy compress(NCPolicy defaultPolicy, PolicyMask[] policies) {
      // drop the policies that are the same as the default
      int newCount = 0;
      for (int i = 0; i < policies.length; i++) {
        if (policies[i].policy != defaultPolicy) {
          policies[newCount] = policies[i];
          newCount++;
        }
      }
      if (newCount == 0) return defaultPolicy;

      // Sort the policies by the mask and name to normalize the order and give priority to the
      // presumably more common policies first
      Arrays.sort(policies, 0, newCount, PolicyMask.COMPARATOR);

      NCPolicy composite;
      if (newCount == 1) composite = new Composite1(defaultPolicy, policies);
      else if (newCount == 2) composite = new Composite2(defaultPolicy, policies);
      else if (newCount == 3) composite = new Composite3(defaultPolicy, policies);
      else throw new GraphInInvalidState("Unexpected number of policies in composite");
      return policyCache.computeIfAbsent(composite, key -> key);
    }

    @Override
    public NCPolicy switchPolicy(PropertyNode<?> key) {
      var mask = key.scenarioStack().pathMask();
      var policy = defaultPolicy;
      for (PolicyMask policyMask : policies) {
        if ((mask & policyMask.mask) != 0) {
          policy = policyMask.policy;
          break;
        }
      }
      // Switch policy according to the underlying policy
      // Remember that XSFT policy has it's own logic that we can't ignore it!
      return policy.switchPolicy(key);
    }

    @Override
    public boolean contains(NCPolicy policy) {
      return defaultPolicy == policy || Arrays.stream(policies).anyMatch(pm -> pm.policy == policy);
    }
  }

  static class Composite1 extends NCPolicyComposite {
    final NCPolicy policy;
    final long mask;

    Composite1(NCPolicy defaultPolicy, CompositeN.PolicyMask[] pm) {
      super(defaultPolicy);
      this.policy = pm[0].policy;
      this.mask = pm[0].mask;
    }

    @Override
    CompositeN toCompositeN() {
      return new CompositeN(defaultPolicy, this.policy, this.mask);
    }

    @Override
    public NCPolicy switchPolicy(PropertyNode<?> key) {
      var r = (key.scenarioStack().pathMask() & mask) != 0 ? policy : defaultPolicy;
      return r.switchPolicy(key);
    }

    @Override
    public boolean contains(NCPolicy policy) {
      return defaultPolicy == policy || this.policy == policy;
    }

    @Override
    public int hashCode() {
      return Objects.hash(policy, mask);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      var that = (Composite1) obj;
      return defaultPolicy == that.defaultPolicy && policy == that.policy && mask == that.mask;
    }
  }

  static class Composite2 extends NCPolicyComposite {
    private final NCPolicy policy1;
    private final NCPolicy policy2;
    private final long mask1;
    private final long mask2;

    Composite2(NCPolicy defaultPolicy, CompositeN.PolicyMask[] pm) {
      super(defaultPolicy);
      this.policy1 = pm[0].policy;
      this.mask1 = pm[0].mask;
      this.policy2 = pm[1].policy;
      this.mask2 = pm[1].mask;
    }

    @Override
    public NCPolicy switchPolicy(PropertyNode<?> key) {
      var resultingPolicy = defaultPolicy;
      var mask = key.scenarioStack().pathMask();
      if ((mask & mask1) != 0) resultingPolicy = policy1;
      else if ((mask & mask2) != 0) resultingPolicy = policy2;
      return resultingPolicy.switchPolicy(key);
    }

    @Override
    CompositeN toCompositeN() {
      return new CompositeN(
          defaultPolicy,
          new PolicyMask[] {new PolicyMask(policy1, mask1), new PolicyMask(policy2, mask2)});
    }

    @Override
    public boolean contains(NCPolicy policy) {
      return defaultPolicy == policy || policy1 == policy || policy2 == policy;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      var that = (Composite2) obj;
      return defaultPolicy == that.defaultPolicy
          && policy1 == that.policy1
          && mask1 == that.mask1
          && policy2 == that.policy2
          && mask2 == that.mask2;
    }

    @Override
    public int hashCode() {
      return Objects.hash(policy1, policy2, mask1, mask2);
    }
  }

  static class Composite3 extends NCPolicyComposite {
    private final NCPolicy policy1;
    private final NCPolicy policy2;
    private final NCPolicy policy3;
    private final long mask1;
    private final long mask2;
    private final long mask3;

    Composite3(NCPolicy defaultPolicy, CompositeN.PolicyMask[] pm) {
      super(defaultPolicy);
      this.policy1 = pm[0].policy;
      this.mask1 = pm[0].mask;
      this.policy2 = pm[1].policy;
      this.mask2 = pm[1].mask;
      this.policy3 = pm[2].policy;
      this.mask3 = pm[2].mask;
    }

    @Override
    public NCPolicy switchPolicy(PropertyNode<?> key) {
      var resultingPolicy = defaultPolicy;
      var mask = key.scenarioStack().pathMask();
      if ((mask & mask1) != 0) resultingPolicy = policy1;
      else if ((mask & mask2) != 0) resultingPolicy = policy2;
      else if ((mask & mask3) != 0) resultingPolicy = policy3;
      return resultingPolicy.switchPolicy(key);
    }

    @Override
    CompositeN toCompositeN() {
      return new CompositeN(
          defaultPolicy,
          new PolicyMask[] {
            new PolicyMask(policy1, mask1),
            new PolicyMask(policy2, mask2),
            new PolicyMask(policy3, mask3)
          });
    }

    @Override
    public boolean contains(NCPolicy policy) {
      return defaultPolicy == policy || policy1 == policy || policy2 == policy || policy3 == policy;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      var that = (Composite3) obj;
      return defaultPolicy == that.defaultPolicy
          && policy1 == that.policy1
          && mask1 == that.mask1
          && policy2 == that.policy2
          && mask2 == that.mask2
          && policy3 == that.policy3
          && mask3 == that.mask3;
    }

    @Override
    public int hashCode() {
      return Objects.hash(policy1, policy2, policy3, mask1, mask2, mask3);
    }
  }
}
