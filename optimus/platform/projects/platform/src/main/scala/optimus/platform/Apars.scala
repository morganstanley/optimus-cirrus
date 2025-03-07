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

trait Apars {

  @node def apar[T1, T2](x1: => T1, x2: => T2): (T1, T2) =
    apar$NF(
      asNode { () =>
        x1
      },
      asNode { () =>
        x2
      })
  @node def apar$NF[T1, T2](x1: NodeFunction0[T1], x2: NodeFunction0[T2]): (T1, T2) = (x1(), x2())

  @node def apar[T1, T2, T3](x1: => T1, x2: => T2, x3: => T3): (T1, T2, T3) =
    apar$NF(
      asNode { () =>
        x1
      },
      asNode { () =>
        x2
      },
      asNode { () =>
        x3
      })
  @node def apar$NF[T1, T2, T3](x1: NodeFunction0[T1], x2: NodeFunction0[T2], x3: NodeFunction0[T3]): (T1, T2, T3) =
    (x1(), x2(), x3())

  @node def apar[T1, T2, T3, T4](x1: => T1, x2: => T2, x3: => T3, x4: => T4): (T1, T2, T3, T4) =
    apar$NF(
      asNode { () =>
        x1
      },
      asNode { () =>
        x2
      },
      asNode { () =>
        x3
      },
      asNode { () =>
        x4
      })
  @node def apar$NF[T1, T2, T3, T4](
      x1: NodeFunction0[T1],
      x2: NodeFunction0[T2],
      x3: NodeFunction0[T3],
      x4: NodeFunction0[T4]): (T1, T2, T3, T4) =
    (x1(), x2(), x3(), x4())

  @node def apar[T1, T2, T3, T4, T5](x1: => T1, x2: => T2, x3: => T3, x4: => T4, x5: => T5): (T1, T2, T3, T4, T5) =
    apar$NF(
      asNode { () =>
        x1
      },
      asNode { () =>
        x2
      },
      asNode { () =>
        x3
      },
      asNode { () =>
        x4
      },
      asNode { () =>
        x5
      })
  @node def apar$NF[T1, T2, T3, T4, T5](
      x1: NodeFunction0[T1],
      x2: NodeFunction0[T2],
      x3: NodeFunction0[T3],
      x4: NodeFunction0[T4],
      x5: NodeFunction0[T5]): (T1, T2, T3, T4, T5) =
    (x1(), x2(), x3(), x4(), x5())

  @node def apar[T1, T2, T3, T4, T5, T6](
      x1: => T1,
      x2: => T2,
      x3: => T3,
      x4: => T4,
      x5: => T5,
      x6: => T6): (T1, T2, T3, T4, T5, T6) =
    apar$NF(
      asNode { () =>
        x1
      },
      asNode { () =>
        x2
      },
      asNode { () =>
        x3
      },
      asNode { () =>
        x4
      },
      asNode { () =>
        x5
      },
      asNode { () =>
        x6
      })
  @node def apar$NF[T1, T2, T3, T4, T5, T6](
      x1: NodeFunction0[T1],
      x2: NodeFunction0[T2],
      x3: NodeFunction0[T3],
      x4: NodeFunction0[T4],
      x5: NodeFunction0[T5],
      x6: NodeFunction0[T6]): (T1, T2, T3, T4, T5, T6) =
    (x1(), x2(), x3(), x4(), x5(), x6())

  @node def apar[T1, T2, T3, T4, T5, T6, T7](
      x1: => T1,
      x2: => T2,
      x3: => T3,
      x4: => T4,
      x5: => T5,
      x6: => T6,
      x7: => T7): (T1, T2, T3, T4, T5, T6, T7) =
    apar$NF(
      asNode { () =>
        x1
      },
      asNode { () =>
        x2
      },
      asNode { () =>
        x3
      },
      asNode { () =>
        x4
      },
      asNode { () =>
        x5
      },
      asNode { () =>
        x6
      },
      asNode { () =>
        x7
      })
  @node def apar$NF[T1, T2, T3, T4, T5, T6, T7](
      x1: NodeFunction0[T1],
      x2: NodeFunction0[T2],
      x3: NodeFunction0[T3],
      x4: NodeFunction0[T4],
      x5: NodeFunction0[T5],
      x6: NodeFunction0[T6],
      x7: NodeFunction0[T7]): (T1, T2, T3, T4, T5, T6, T7) =
    (x1(), x2(), x3(), x4(), x5(), x6(), x7())

  @node def apar[T1, T2, T3, T4, T5, T6, T7, T8](
      x1: => T1,
      x2: => T2,
      x3: => T3,
      x4: => T4,
      x5: => T5,
      x6: => T6,
      x7: => T7,
      x8: => T8): (T1, T2, T3, T4, T5, T6, T7, T8) =
    apar$NF(
      asNode { () =>
        x1
      },
      asNode { () =>
        x2
      },
      asNode { () =>
        x3
      },
      asNode { () =>
        x4
      },
      asNode { () =>
        x5
      },
      asNode { () =>
        x6
      },
      asNode { () =>
        x7
      },
      asNode { () =>
        x8
      })
  @node def apar$NF[T1, T2, T3, T4, T5, T6, T7, T8](
      x1: NodeFunction0[T1],
      x2: NodeFunction0[T2],
      x3: NodeFunction0[T3],
      x4: NodeFunction0[T4],
      x5: NodeFunction0[T5],
      x6: NodeFunction0[T6],
      x7: NodeFunction0[T7],
      x8: NodeFunction0[T8]): (T1, T2, T3, T4, T5, T6, T7, T8) =
    (x1(), x2(), x3(), x4(), x5(), x6(), x7(), x8())

  @node def apar[T1, T2, T3, T4, T5, T6, T7, T8, T9](
      x1: => T1,
      x2: => T2,
      x3: => T3,
      x4: => T4,
      x5: => T5,
      x6: => T6,
      x7: => T7,
      x8: => T8,
      x9: => T9): (T1, T2, T3, T4, T5, T6, T7, T8, T9) =
    apar$NF(
      asNode { () =>
        x1
      },
      asNode { () =>
        x2
      },
      asNode { () =>
        x3
      },
      asNode { () =>
        x4
      },
      asNode { () =>
        x5
      },
      asNode { () =>
        x6
      },
      asNode { () =>
        x7
      },
      asNode { () =>
        x8
      },
      asNode { () =>
        x9
      })
  @node def apar$NF[T1, T2, T3, T4, T5, T6, T7, T8, T9](
      x1: NodeFunction0[T1],
      x2: NodeFunction0[T2],
      x3: NodeFunction0[T3],
      x4: NodeFunction0[T4],
      x5: NodeFunction0[T5],
      x6: NodeFunction0[T6],
      x7: NodeFunction0[T7],
      x8: NodeFunction0[T8],
      x9: NodeFunction0[T9]): (T1, T2, T3, T4, T5, T6, T7, T8, T9) =
    (x1(), x2(), x3(), x4(), x5(), x6(), x7(), x8(), x9())

  @async def aseq[T1, T2](x1: => T1, x2: => T2): (T1, T2) =
    aseq$NF(
      asNode { () =>
        x1
      },
      asNode { () =>
        x2
      })
  @async def aseq$NF[T1, T2](x1: AsyncFunction0[T1], x2: AsyncFunction0[T2]): (T1, T2) = (x1(), x2())

  @async def aseq[T1, T2, T3](x1: => T1, x2: => T2, x3: => T3): (T1, T2, T3) =
    aseq$NF(
      asNode { () =>
        x1
      },
      asNode { () =>
        x2
      },
      asNode { () =>
        x3
      })
  @async def aseq$NF[T1, T2, T3](x1: AsyncFunction0[T1], x2: AsyncFunction0[T2], x3: AsyncFunction0[T3]): (T1, T2, T3) =
    (x1(), x2(), x3())

}
