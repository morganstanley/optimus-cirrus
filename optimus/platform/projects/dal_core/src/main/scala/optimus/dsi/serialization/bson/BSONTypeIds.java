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
package optimus.dsi.serialization.bson;

/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// TODO (OPTIMUS-57177): license needs reformatting in order to publish this code as open-source
/**
 * Contains byte representations of all the BSON types (see the <a
 * href="http://bsonspec.org/spec.html">BSON Specification</a>).
 */
public class BSONTypeIds {

  // ---- basics ----

  public static final byte Eoo = 0;
  public static final byte Number = 1;
  public static final byte String = 2;
  public static final byte Object = 3;
  public static final byte Array = 4;
  public static final byte Binary = 5;
  public static final byte Undefined = 6;
  public static final byte Oid = 7;
  public static final byte Boolean = 8;
  public static final byte Date = 9;
  public static final byte Null = 10;
  public static final byte Regex = 11;
  public static final byte Ref = 12;
  public static final byte Code = 13;
  public static final byte Symbol = 14;
  public static final byte CodeScope = 15;
  public static final byte NumberInt = 16;
  public static final byte Timestamp = 17;
  public static final byte NumberLong = 18;

  public static final byte MinKey = -1;
  public static final byte MaxKey = 127;

  // --- binary types
  /*
     these are binary types
     so the format would look like
     <Binary><name><BINARY_TYPE><...>
  */

  public static final byte BinGeneral = 0;
  public static final byte BinFunc = 1;
  public static final byte BinBinary = 2;
  public static final byte BinUuid = 3;
}
