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
package optimus.platform.storable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serial;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import msjava.base.util.uuid.MSUuid;
import msjava.slf4jutils.scalalog.Logger;
import optimus.entity.ModuleEntityInfo;
import optimus.graph.AuditTrace;
import optimus.graph.GraphInInvalidState;
import optimus.graph.Settings;
import optimus.platform.EvaluationContext$;
import optimus.platform.TemporalContext;
import optimus.platform.annotations.internal.EntityMetaDataAnnotation;
import optimus.platform.pickling.PickledInputStream;
import optimus.platform.pickling.PickledOutputStream;
import optimus.platform.pickling.ReflectiveEntityPickling$;

@EntityMetaDataAnnotation(isStorable = true)
public abstract class EntityImpl implements Entity {
  private static final VarHandle flavor_mh;

  static {
    try {
      var lookup = MethodHandles.lookup();
      flavor_mh = lookup.findVarHandle(EntityImpl.class, "entityFlavor", EntityFlavor.class);
    } catch (IllegalAccessException | NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unused") //  entityFlavor mutated via flavor_mh
  private volatile EntityFlavor entityFlavor;

  private transient int $hashCode;
  private transient Object _localCache;

  public EntityImpl(PickledInputStream is, StorageInfo info, EntityReference eref) {
    var flavor = EntityFlavor$.MODULE$.apply(is, info, eref);
    initEntityFlavor(flavor);
    // if (gs.debugging && !$info.valsAreInstrumented) EntityAgent.injectValPreCalls(this)
    if (Settings.auditing) AuditTrace.visit(this);
  }

  public EntityImpl() {
    initEntityFlavor(DefaultAppliedEF$.MODULE$);
    if (Settings.auditing) AuditTrace.visit(this);
  }

  public final void mkUnique() {
    if (entityFlavor != DefaultAppliedEF$.MODULE$) throw new GraphInInvalidState();
    EvaluationContext$.MODULE$.verifyImpure();
    initEntityFlavor(DefaultUniqueEF$.MODULE$);
  }

  @Override
  public void pickle(PickledOutputStream out) {
    ReflectiveEntityPickling$.MODULE$.instance().pickle(this, out);
  }

  /** Valid during construction only! */
  final void initEntityFlavor(EntityFlavor flavor) {
    flavor_mh.set(this, flavor);
  }

  public final EntityFlavor entityFlavorInternal() {
    return (EntityFlavor) flavor_mh.getVolatile(this);
  }

  @Override
  public String toString() {
    return EntityInternals$.MODULE$.entityInfoString(this);
  }

  public static void setLocalCache(Entity e, Object value) {
    ((EntityImpl) e)._localCache = value;
  }

  public static Object getLocalCache(Entity e) {
    return ((EntityImpl) e)._localCache;
  }

  /**
   * Entities are equal if (non negotiable hence the method is final) 1) they refer to the same
   * object, or 2) neither is temporary and both the key and temporal load context are equal
   */
  @Override
  public final boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj instanceof EntityImpl that) {
      if (this.getClass() != that.getClass()) return false;
      // different creation universe won't equal
      if (this.entityFlavor.universe() != that.entityFlavor.universe()) return false;
      if ($hashCode != 0 && that.$hashCode != 0 && $hashCode != that.$hashCode) return false;
      return entityFlavor.entityEquals(this, that);
    } else return false;
  }

  @Override
  public final int hashCode() {
    var hashCode = $hashCode;
    if (hashCode == 0) {
      hashCode = entityFlavor.entityHashCode(this);
      if (hashCode == 0) hashCode = -1;
      $hashCode = hashCode;
    }
    return hashCode;
  }

  /** Just a forwarder to hide from a public view */
  public static int argsHash(Entity e) {
    return e.argsHash();
  }

  /** Just a forwarder to hide from a public view */
  public static boolean argsEquals(Entity e1, Entity e2) {
    return e1.argsEquals(e2);
  }

  /**
   * Prepares the entity to have DAL flavor mutated into it and returns the DALEntityFlavor. If the
   * entity already had DALEntityFlavor (or a HybridEntityFlavor which contains one) just returns
   * it. IF the entity did not, it atomically upgrades the flavor to a suitable HybridEntityFlavor
   * first (in practice this happens when and only when an applied or unique entity is persisted in
   * a legacy persist block)
   */
  DALEntityFlavor prepareMutate() {
    var localEntityFlavor = entityFlavor;
    // n.b. We loop on the CAS because we may race against the CAS in writeObject() which upgrades
    // from
    // DefaultUniqueEF to FrozenUniqueEntityFlavor
    var success = false;
    do {
      if (localEntityFlavor.dalEntityFlavor() != null) break;

      var dalef = new DALEntityFlavor();
      EntityFlavor e;
      if (localEntityFlavor == DefaultAppliedEF$.MODULE$) {
        e = new HybridAppliedEntityFlavor(dalef);
      } else if (localEntityFlavor == DefaultUniqueEF$.MODULE$) {
        // strictly we only need to hybridize, not freeze, here, but it's simpler to just do both
        // because then there are fewer possible states and fewer classes needed
        // (and hybridization is much rarer than freezing)
        e = new HybridUniqueEntityFlavor(dalef, MSUuid.generateJavaUUID(), hashCode());
      } else if (localEntityFlavor instanceof FrozenUniqueEntityFlavor f) {
        e = new HybridUniqueEntityFlavor(dalef, f.uuid(), f.frozenHashCode());
      } else throw new IllegalStateException("Unknown default EntityFlavor type");

      success = flavor_mh.weakCompareAndSet(this, localEntityFlavor, e);
      localEntityFlavor = entityFlavor;
    } while (!success);
    return localEntityFlavor.dalEntityFlavor();
  }

  @Override
  public Logger log() {
    return $info().log();
  }

  @Override
  public final EntityReference $permReference() {
    return entityFlavor.dal$entityRef();
  }

  @Override
  public final EntityReference dal$entityRef() {
    return entityFlavor.dal$entityRef();
  }

  @Override
  public final StorageInfo dal$storageInfo() {
    return entityFlavor.dal$storageInfo();
  }

  @Override
  public final TemporalContext dal$temporalContext() {
    return entityFlavor.dal$temporalContext();
  }

  @Override
  public final TemporalContext dal$loadContext() {
    return entityFlavor.dal$loadContext();
  }

  @Override
  public boolean $inline() {
    return false;
  }

  @Override
  public final boolean dal$isTemporary() {
    return entityFlavor.dal$isTemporary();
  }

  @Serial
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    // $info is defined as A.info for entity A.
    // A call to $info ensures the initialization of the entity's companion.
    $info();
    in.defaultReadObject();
  }

  @Serial
  public Object writeReplace() {
    if ($info() instanceof ModuleEntityInfo)
      return new ModuleEntitySerializationToken(getClass().getName());
    else if (dal$isTemporary() || $inline()) {
      // if it's a plain vanilla unfrozen unique entity we must stamp in a frozen UUID and hashcode
      // so that it keeps the same identity after deserialization. If not, it's either not unique
      // (i.e. is heap or DAL), or else is already frozen (either with or without the hybrid DAL
      // nature). If we lose the CAS, we either lost it to another call here, or to prepareMutate()
      // which already does a superset of this logic, so we don't care either way.
      if (entityFlavor == DefaultUniqueEF$.MODULE$) {
        var frozenFlavor = new FrozenUniqueEntityFlavor(MSUuid.generateJavaUUID(), hashCode());
        EntityImpl.flavor_mh.compareAndSet(this, DefaultUniqueEF$.MODULE$, frozenFlavor);
      }
      return this;
    } else {
      // The major use of serialization is for distribution.  In that case we are sending down
      // ReferenceEntityTokens in the middle of a giant serialized blob.
      // During deserialization we want to avoid a lot of synchronous DAL
      // calls to rehydrate the entities, so we gather all referenced entities into a buffer so that
      // we can prefetch.
      SerializationReferenceCollector$.MODULE$.add(this);
      return ReferenceEntityToken$.MODULE$.apply(
          entityFlavor.dal$entityRef(), entityFlavor.dal$temporalContext());
    }
  }
}
