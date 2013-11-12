/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.cache;

import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class HCDrillSerializableImpl implements DataSerializable {

  private DrillSerializable obj;

  public HCDrillSerializableImpl() {
  }

  public HCDrillSerializableImpl(DrillSerializable obj) {
    this.obj = obj;
  }

  public void readData(DataInput arg0) throws IOException {
    obj.read(arg0);
  }

  public void writeData(DataOutput arg0) throws IOException {
    obj.write(arg0);
  }

  public DrillSerializable get() {
    return obj;
  }

  public static HCDrillSerializableImpl getWrapper(DrillSerializable value, Class clazz) {
    if (clazz.equals(VectorWrap.class)) {
      return new HCSerializableImplClasses.VectorWrapSerializable(value);
    } else {
      throw new UnsupportedOperationException("HCDrillSerializableImpl not implemented for " + clazz);
    }
  }
}
