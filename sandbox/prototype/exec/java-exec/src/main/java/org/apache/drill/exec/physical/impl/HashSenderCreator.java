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

package org.apache.drill.exec.physical.impl;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.HashPartitionSender;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.bit.BitTunnel;

import java.util.ArrayList;
import java.util.List;

public class HashSenderCreator implements RootCreator<HashPartitionSender> {

  @Override
  public RootExec getRoot(FragmentContext context,
                          HashPartitionSender config,
                          List<RecordBatch> children) throws ExecutionSetupException {
    assert children != null && children.size() == 1;
    return new HashSenderRootExec(context, children.iterator().next(), config);
  }

  private static class HashSenderRootExec implements RootExec {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HashSenderRootExec.class);
    private RecordBatch incoming;
    private List<BitTunnel> tunnels;
    private ExecProtos.FragmentHandle handle;
    private int recMajor;
    private FragmentContext context;
    private volatile boolean ok = true;

    public HashSenderRootExec(FragmentContext context, RecordBatch batch, HashPartitionSender config) {
      this.incoming = batch;
      this.handle = context.getHandle();
      this.recMajor = config.getOppositeMajorFragmentId();
      this.tunnels = new ArrayList<>();
      for (DrillbitEndpoint endpoint : config.getDestinations()) 
        this.tunnels.add(context.getCommunicator().getTunnel(endpoint));
      this.context = context;
    }

    @Override
    public boolean next() {
      if (!ok) {
        incoming.kill();
        return false;
      }

      RecordBatch.IterOutcome out = incoming.next();
      logger.debug("HashSender.next() sending status {}", out);
      switch(out){
        case STOP:
        case NONE:
        {
          FragmentWritableBatch batch = new FragmentWritableBatch(true,
                                                               handle.getQueryId(),
                                                               handle.getMajorFragmentId(),
                                                               handle.getMinorFragmentId(),
                                                               recMajor,
                                                               0,
                                                               incoming.getWritableBatch());
          getTunnelForBatch(batch).sendRecordBatch(new RecordSendFailure(), context, batch);
          return false;
        }
        case OK:
        case OK_NEW_SCHEMA:
        {
          FragmentWritableBatch batch = new FragmentWritableBatch(false, 
                                                                  handle.getQueryId(),
                                                                  handle.getMajorFragmentId(),
                                                                  handle.getMinorFragmentId(),
                                                                  recMajor,
                                                                  0,
                                                                  incoming.getWritableBatch());
          getTunnelForBatch(batch).sendRecordBatch(new RecordSendFailure(), context, batch);
          return true;
        }
        case NOT_YET:
        default:
          throw new IllegalStateException();
      }

    }

    private BitTunnel getTunnelForBatch(FragmentWritableBatch batch) {
      return tunnels.iterator().next();
    }

    @Override
    public void stop() {
      ok = false;
    }

    private class RecordSendFailure extends BaseRpcOutcomeListener<GeneralRPCProtos.Ack> {

      @Override
      public void failed(RpcException ex) {
        context.fail(ex);
        stop();
      }

      @Override
      public void success(GeneralRPCProtos.Ack value) {
        if(value.getOk()) return;

        logger.error("Downstream fragment was not accepted.  Stopping future sends.");
        // if we didn't get ack ok, we'll need to kill the query.
        context.fail(new RpcException("Failed to send fragment.  Downstream fragment batch not accepted."));
        stop();
      }

    }

  }

}
