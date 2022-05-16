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
package optimus;

final public class DoubleBox {
    private DoubleBox() { }

    private static final Double DOUBLE_ZERO = Double.valueOf(0.0);
    private static final long DOUBLE_ZERO_BITS = Double.doubleToLongBits(0.0);
    private static final Double DOUBLE_NEG_ZERO = Double.valueOf(-0.0);

    public static final Double boxToDouble(double d) {
        // Double differentiates zero / negative zero, but primitive == does not
        // also note that Double.NaN.equals(Double.NaN) is true, unlike primitive
        if (d == 0.0) {
            if (Double.doubleToLongBits(d) == DOUBLE_ZERO_BITS) return DOUBLE_ZERO;
            else return DOUBLE_NEG_ZERO;
        } else return Double.valueOf(d);
    }

}
