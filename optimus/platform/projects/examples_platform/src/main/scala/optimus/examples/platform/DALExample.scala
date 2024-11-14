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
package optimus.examples.platform

import optimus.utils.datetime.ZoneIds
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZonedDateTime

import msjava.slf4jutils.scalalog.getLogger
import optimus.platform._
import optimus.platform.temporalSurface.TemporalSurfaceDefinition.FixedLeafContext
import optimus.platform.temporalSurface.TemporalSurfaceMatchers

@stored @entity class Foo(
    val who: String,
    val when: ZonedDateTime,
    val id: Int,
    @node(tweak = true) val value: String) {
  @key def key = (who, when, id)
}

@event class FooEvent()

object DALExample extends LegacyOptimusApp[OptimusAppCmdLine] {
  private val log = getLogger(this)

  val who = System.getProperty("user.name")
  val when = ZonedDateTime.now

  def makeFoo(id: Int) = Foo(who, when, id, "A")

  val vt1 = LocalDate.of(2016, 1, 1).atStartOfDay().atZone(ZoneIds.UTC).toInstant
  val vt2 = LocalDate.of(2016, 1, 2).atStartOfDay().atZone(ZoneIds.UTC).toInstant
  val vt3 = LocalDate.of(2016, 1, 3).atStartOfDay().atZone(ZoneIds.UTC).toInstant
  val vt4 = LocalDate.of(2016, 1, 4).atStartOfDay().atZone(ZoneIds.UTC).toInstant

  example1()
  example2()
  example3()
  example4()
  example5()
  example6()
  example7()
  example8()
  example9()
  example10()
  example11()

  def example1(): Unit = {
    log.info("example1 - inserting at v2 only covers period of vt2 to vt3 due to a pre-existing entity at vt3")
    val (fa, tt1) = putFoo("P1", makeFoo(1), vt1)
    getFoo("G1", 1, vt1, tt1)
    val (fb, tt2) = putFoo("P2", fa, vt3, Some("B"))
    getFoo("G2", 1, vt3, tt2)
    val (fc, tt3) = putFoo("P3", fb, vt2, Some("C"))
    getFoo("G3", 1, vt2, tt3)
    getFoo("G4", 1, vt3, tt3)
  }

  def example2(): Unit = {
    log.info("example2 - as per example1, demonstrating behaviour is identical for upsert")
    val (fa, tt1) = upsertFoo("U1", makeFoo(2), vt1)
    getFoo("G1", 2, vt1, tt1)
    val (fb, tt2) = upsertFoo("U2", makeFoo(2), vt3, Some("B"))
    getFoo("G2", 2, vt3, tt2)
    val (fc, tt3) = upsertFoo("U3", makeFoo(2), vt2, Some("C"))
    getFoo("G3", 2, vt2, tt3)
    getFoo("G4", 2, vt3, tt3)
  }

  def example3(): Unit = {
    log.info(
      "example3 - when you fully invalidate the only entry, you cannot use put (because there is no valid entity to load at the latest tt)")
    val (fa, tt1) = putFoo("P1", makeFoo(3), vt1)
    getFoo("G1", 3, vt1, tt1)
    val tt2 = invalidateFoo("I1", fa, vt1)
    getFoo("G2", 3, vt1, tt2)
    val (fb, tt3) = upsertFoo("S2", makeFoo(3), vt1, Some("B"))
    getFoo("G3", 3, vt1, tt3)
  }

  def example4(): Unit = {
    log.info(
      "example4 - demonstrating saves and invalidates, need to reload non-invalidated entity version at tt>=last update, in order to use put/invalidate again")
    val (fa, tt1) = putFoo("P1", makeFoo(4), vt1)
    getFoo("G1", 4, vt1, tt1)
    val (fb, tt2) = putFoo("P2", fa, vt2, Some("B"))
    getFoo("G2", 4, vt1, tt2)
    getFoo("G3", 4, vt2, tt2)
    val tt3 = invalidateFoo("I1", fb, vt2) // have to pass in version with latest tt like with put
    val fa2 = getFoo("G4", 4, vt1, tt3)
    val fb2 = getFoo("G5", 4, vt2, tt3)
    // need to load use fa2 (any entity which has tt > latest update)
    val (fc, tt4) = putFoo("P3", fa2, vt2, Some("C"))
    getFoo("G6", 4, vt1, tt4)
    getFoo("G7", 4, vt2, tt4)
  }

  def example5(): Unit = {
    log.info(
      "example5 - when invalidating an entity, if you provide a vt outside range of provided foo, it modifies another version (rectangle)")
    val (fa, tt1) = putFoo("P1", makeFoo(5), vt1)
    getFoo("G1", 5, vt1, tt1)
    val (fb, tt2) = putFoo("P2", fa, vt2, Some("B"))
    getFoo("G2", 5, vt1, tt2)
    getFoo("G3", 5, vt2, tt2)
    val tt3 = invalidateFoo("I1", fb, vt1)
    getFoo("G4", 5, vt1, tt3)
    getFoo("G5", 5, vt2, tt3)
  }

  def example6(): Unit = {
    log.info("example6 - invalidate uses vt of event, it doesn't invalidate the whole entity version provided")
    val (fa, tt1) = putFoo("P1", makeFoo(6), vt1)
    getFoo("G1", 6, vt1, tt1)
    val (fb, tt2) = putFoo("P2", fa, vt2, Some("B"))
    getFoo("G2", 6, vt1, tt2)
    getFoo("G3", 6, vt2, tt2)
    val tt3 = invalidateFoo("I1", fb, vt3) // have to pass in version with latest tt like with put
    getFoo("G4", 6, vt1, tt3)
    val fb2 = getFoo("G5", 6, vt2, tt3)
    val (fc, tt4) = putFoo("P3", fb2, vt2, Some("C"))
    getFoo("G6", 6, vt1, tt4)
    getFoo("G7", 6, vt2, tt4)
  }

  def example7(): Unit = {
    log.info("example7 - reload valid entity at latest tt in order to use for another put")
    val (fa, tt1) = putFoo("P1", makeFoo(7), vt1)
    getFoo("G1", 7, vt1, tt1)
    val (fb, tt2) = putFoo("P2", fa, vt2, Some("B"))
    getFoo("G2", 7, vt1, tt2)
    getFoo("G3", 7, vt2, tt2)
    val tt3 = invalidateFoo("I1", fb, vt1)
    getFoo("G4", 7, vt1, tt3)
    val fb2 = getFoo("G5", 7, vt2, tt3)
    log.info(s"fb ${fb.validTime}")
    log.info(s"fb2 ${fb.validTime}")
    // need to reload fb2 - even though it hasn't appeared to change in any way
    val (fc, tt4) = putFoo("P3", fb2, vt2, Some("C"))
    getFoo("G6", 7, vt1, tt4)
    getFoo("G7", 7, vt2, tt4)
  }

  def example8(): Unit = {
    log.info(
      "example8 - if you invalidate an entity, and then insert another entity before the invalidation time, that entity is also cut off by the invalidate")
    val (fa, tt1) = putFoo("P1", makeFoo(8), vt1)
    getFoo("G1", 8, vt1, tt1)
    getFoo("G2", 8, vt2, tt1)
    val tt2 = invalidateFoo("I1", fa, vt2)
    val fa2 = getFoo("G3", 8, vt1, tt2)
    getFoo("G4", 8, vt2, tt2)
    val (fb, tt3) = putFoo("P3", fa2, vt1, Some("B"))
    getFoo("G5", 8, vt1, tt3)
    getFoo("G6", 8, vt2, tt3)
  }

  def example9(): Unit = {
    log.info("example9 - if vt == tt (typical use case), which version contains latest lock token")
    val (fa, tt1) = putFoo("P1", makeFoo(9), vt1)
    getFoo("G1", 9, vt1, tt1)
    val tt2 = invalidateFoo("I1", fa, vt2)
    val fa2 = getFoo("G2", 9, vt1, tt2)
    getFoo("G3", 9, vt2, tt2)
    val (fb, tt3) = putFoo("P2", fa2, vt2, Some("B"))
    val fa3 = getFoo("G4", 9, vt1, tt3)
    val fb3 = getFoo("G5", 9, vt2, tt3)
    // can use fa3 as it was loaded at transaction time of last update (lock token is valid), even though tx time is prior to latest
    val (fc, tt4) = putFoo("P3", fa3, vt3, Some("C"))
  }

  def example10(): Unit = {
    log.info("example10 - using validTimeline to find latest valid version?")
    val (fa, tt1) = putFoo("P1", makeFoo(10), vt1)
    getFoo("G1", 9, vt1, tt1)
    val tt2 = invalidateFoo("I1", fa, vt2)
    // can't use validTimeline as must be against a DAL entity (not a heap entity)
    // val vts = makeFoo(10).validTimeline.validTimes
    // log.info(vts)
  }

  def example11(): Unit = {
    log.info("example11 - what is in validTimeline and transactionTimeline (these are referentially transparent!)")
    val (fa, tt1) = putFoo("P1", makeFoo(11), vt1)
    val (fb, tt2) = putFoo("P2", fa, vt3, Some("B"))
    val (fc, tt3) = putFoo("P3", fb, vt2, Some("C"))
    log.info(s"fa valid timeline ${fa.validTimeline.validTimes.toList.sorted}")
    log.info(s"fa transaction timeline ${fa.transactionTimeline.transactionTimes.sorted}")
    log.info(s"fb valid timeline ${fb.validTimeline.validTimes.toList.sorted}")
    log.info(s"fb transaction timeline ${fb.transactionTimeline.transactionTimes.sorted}")
    log.info(s"fc valid timeline ${fc.validTimeline.validTimes.toList.sorted}")
    log.info(s"fc transaction timeline ${fc.transactionTimeline.transactionTimes.sorted}")
  }

  def putFoo(printKey: String, fIn: Foo, vt: Instant, update: Option[String] = None): (Foo, Instant) = {
    given(storeContext := vt) {
      val result = update
        .map(v =>
          given(fIn.value := v) {
            newTransaction {
              newEvent(FooEvent.uniqueInstance()) {
                DAL.put(fIn)
              }
            }
          })
        .getOrElse(newTransaction {
          newEvent(FooEvent.uniqueInstance()) {
            DAL.put(fIn)
          }
        })
      val tt = result.tt
      val fOut = result.getEntityReferenceHolder(fIn).get.payload
      log.info(s"Put $printKey at vt=$vt, tt=$tt, value in=${fIn.value}, out=${fOut.value}")
      (fOut, tt)
    }
  }

  def upsertFoo(printKey: String, fIn: Foo, vt: Instant, update: Option[String] = None): (Foo, Instant) = {
    given(storeContext := vt) {
      val result = update
        .map(v =>
          given(fIn.value := v) {
            newTransaction {
              newEvent(FooEvent.uniqueInstance()) {
                DAL.upsert(fIn)
              }
            }
          })
        .getOrElse(newTransaction {
          newEvent(FooEvent.uniqueInstance()) {
            DAL.upsert(fIn)
          }
        })
      val tt = result.tt
      val fOut = result.getEntityReferenceHolder(fIn).get.payload
      log.info(s"Ups $printKey at vt=$vt, tt=$tt, value in=${fIn.value}, out=${fOut.value}")
      (fOut, tt)
    }
  }

  def getFoo(printKey: String, ver: Int, vt: Instant, tt: Instant): Foo = {
    given(loadContext := FixedLeafContext(TemporalSurfaceMatchers.all, vt, tt)) {
      val fOpt = Foo.getOption(who, when, ver)
      if (fOpt.isDefined) {
        val f = fOpt.get
        log.info(
          s"Get $printKey at vt=$vt, tt=$tt, value ${f.value}, entity validTime ${f.validTime}, entity txTime ${f.txTime}")
        f
      } else {
        log.info(s"Get $printKey at vt=$vt, tt=$tt, NONE")
        null
      }
    }
  }

  def invalidateFoo(printKey: String, fIn: Foo, vt: Instant) = {
    given(storeContext := vt) {
      val result = newTransaction {
        newEvent(FooEvent.uniqueInstance()) {
          DAL.invalidate(fIn)
        }
      }
      val tt = result.tt
      log.info(s"Inv $printKey at $tt")
      tt
    }
  }

}
