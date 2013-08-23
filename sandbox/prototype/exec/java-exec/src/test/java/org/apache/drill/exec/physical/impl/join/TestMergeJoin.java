/*******************************************************************************
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
 ******************************************************************************/

package org.apache.drill.exec.physical.impl.join;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.yammer.metrics.MetricRegistry;
import mockit.Injectable;
import mockit.NonStrictExpectations;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.impl.ImplCreator;
import org.apache.drill.exec.physical.impl.SimpleRootExec;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestMergeJoin {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestMergeJoin.class);

  DrillConfig c = DrillConfig.create();

  @Test
  public void simpleEqualityJoin(@Injectable final DrillbitContext bitContext,
                                 @Injectable UserServer.UserClientConnection connection) throws Throwable {

    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry("test");
      bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
    }};

    PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(), CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    PhysicalPlan plan = reader.readPhysicalPlan(Files.toString(FileUtils.getResourceAsFile("/join/merge_join.json"), Charsets.UTF_8));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    int totalRecordCount = 0;
    while (exec.next()) {
      totalRecordCount += exec.getRecordCount();
      for (ValueVector v : exec)
        System.out.print("[" + v.getField().getName() + "]         ");
      System.out.println("\n");
      for (int valueIdx = 0; valueIdx < exec.getRecordCount(); valueIdx++) {
        List<Object> row = new ArrayList();
        for (ValueVector v : exec) {
           row.add(v.getAccessor().getObject(valueIdx));
        }
        for (Object cell : row) {
          if (cell == null) { 
            System.out.print("[NULL]          ");
            continue;
          }
          int len = cell.toString().length();
          System.out.print("[" + cell + "]");
          for (int i = 0; i < (14 - len); ++i)
            System.out.print(" ");
        }
        System.out.println();
      }
      assertEquals(100, exec.getRecordCount());
    }
    System.out.println("Total Record Count: " + totalRecordCount);
    if (context.getFailureCause() != null)
      throw context.getFailureCause();
    assertTrue(!context.isFailed());

  }

  @Test
  public void orderedEqualityJoin(@Injectable final DrillbitContext bitContext,
                                  @Injectable UserServer.UserClientConnection connection) throws Throwable {

    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry("test");
      bitContext.getAllocator(); result = BufferAllocator.getAllocator(c);
    }};

    PhysicalPlanReader reader = new PhysicalPlanReader(c, c.getMapper(),CoordinationProtos.DrillbitEndpoint.getDefaultInstance());
    PhysicalPlan plan = reader.readPhysicalPlan(
        Files.toString(
            FileUtils.getResourceAsFile("/join/merge_single_batch.json"), Charsets.UTF_8)
            .replace("#{LEFT_FILE}", FileUtils.getResourceAsFile("/join/merge_single_batch.left.json").toURI().toString())
            .replace("#{RIGHT_FILE}", FileUtils.getResourceAsFile("/join/merge_single_batch.right.json").toURI().toString()));
    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContext context = new FragmentContext(bitContext, ExecProtos.FragmentHandle.getDefaultInstance(), connection, null, registry);
    SimpleRootExec exec = new SimpleRootExec(ImplCreator.getExec(context, (FragmentRoot) plan.getSortedOperators(false).iterator().next()));

    int totalRecordCount = 0;
    while (exec.next()) {
      System.out.println("got next with record count: " + exec.getRecordCount() + " (total: " + totalRecordCount + ") \n");
      totalRecordCount += exec.getRecordCount();
      for (ValueVector v : exec)
        System.out.print(v.getField().getName() + "       ");
      System.out.println("\n----");
      for (int valueIdx = 0; valueIdx < exec.getRecordCount(); valueIdx++) {
        List<Object> row = new ArrayList();
        for (ValueVector v : exec) {
          row.add(v.getAccessor().getObject(valueIdx));
        }
        for (Object cell : row) {
          if (cell == null) continue;
          int len = cell.toString().length();
          System.out.print(cell);
          for (int i = 0; i < (10 - len); ++i)
            System.out.print(" ");
        }
        System.out.println();
      }
      assertEquals(12, exec.getRecordCount());
    }
    System.out.println("-------------------");
    System.out.println("Total Record Count: " + totalRecordCount);
    if (context.getFailureCause() != null)
      throw context.getFailureCause();
    assertTrue(!context.isFailed());

  }

}



