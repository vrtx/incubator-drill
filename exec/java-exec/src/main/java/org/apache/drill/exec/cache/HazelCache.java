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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.hazelcast.core.*;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.cache.ProtoBufImpl.HWorkQueueStatus;
import org.apache.drill.exec.cache.ProtoBufImpl.HandlePlan;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.proto.ExecProtos.WorkQueueStatus;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.hazelcast.config.Config;

public class HazelCache implements DistributedCache {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HazelCache.class);

  private final String instanceName;
  private HazelcastInstance instance;
  private ITopic<HWorkQueueStatus> workQueueLengths;
  private HandlePlan fragments;
  private Cache<WorkQueueStatus, Integer>  endpoints;
  
  public HazelCache(DrillConfig config) {
    this.instanceName = config.getString(ExecConstants.SERVICE_NAME);
  }

  private class Listener implements MessageListener<HWorkQueueStatus>{

    @Override
    public void onMessage(Message<HWorkQueueStatus> wrapped) {
      logger.debug("Received new queue length message.");
      endpoints.put(wrapped.getMessageObject().get(), 0);
    }
    
  }
  
  public void run() {
    Config c = new Config();
    c.setInstanceName(instanceName);
    instance = getInstanceOrCreateNew(c);
    workQueueLengths = instance.getTopic("queue-length");
    fragments = new HandlePlan(instance);
    endpoints = CacheBuilder.newBuilder().maximumSize(2000).build();
    workQueueLengths.addMessageListener(new Listener());
  }

  private HazelcastInstance getInstanceOrCreateNew(Config c) {
    for (HazelcastInstance instance : Hazelcast.getAllHazelcastInstances()){
      if (instance.getName().equals(this.instanceName))
        return instance;
    }
    try {
    return Hazelcast.newHazelcastInstance(c);
    } catch (DuplicateInstanceNameException e) {
      return getInstanceOrCreateNew(c);
    }
  }

//  @Override
//  public void updateLocalQueueLength(int length) {
//    workQueueLengths.publish(new HWorkQueueStatus(WorkQueueStatus.newBuilder().setEndpoint(endpoint)
//        .setQueueLength(length).setReportTime(System.currentTimeMillis()).build()));
//  }
//
//  @Override
//  public List<WorkQueueStatus> getQueueLengths() {
//    return Lists.newArrayList(endpoints.asMap().keySet());
//  }

  @Override
  public void close() throws IOException {
    this.instance.getLifecycleService().shutdown();
  }

  @Override
  public PlanFragment getFragment(FragmentHandle handle) {
    return this.fragments.get(handle);
  }

  @Override
  public void storeFragment(PlanFragment fragment) {
    fragments.put(fragment.getHandle(), fragment);
  }
  

  @Override
  public <V extends DrillSerializable> MultiMap<V> getMultiMap(Class<V> clazz) {
    return new HCMultiMapImpl(this.instance.getMultiMap(clazz.toString()), clazz);
  }

  @Override
  public <V extends DrillSerializable> Map<V> getMap(Class<V> clazz) {
    return new HCMapImpl(this.instance.getMap(clazz.toString()), clazz);
  }

  @Override
  public Counter getCounter(String name) {
    return new HCCounterImpl(this.instance.getAtomicNumber(name));
  }

  public static class HCMapImpl<V> implements Map<V> {
    private IMap<String, HCDrillSerializableImpl> m;
    private Class<V> clazz;

    public HCMapImpl(IMap m, Class<V> clazz) {
      this.m = m;
      this.clazz = clazz;
    }

    public DrillSerializable get(String key) {
      return m.get(key).get();
    }

    public void put(String key, DrillSerializable value) {
      m.put(key, HCDrillSerializableImpl.getWrapper(value, clazz));
    }

    public void putIfAbsent(String key, DrillSerializable value) {
      m.putIfAbsent(key, HCDrillSerializableImpl.getWrapper(value, clazz));
    }

    public void putIfAbsent(String key, DrillSerializable value, long ttl, TimeUnit timeunit) {
      m.putIfAbsent(key, HCDrillSerializableImpl.getWrapper(value, clazz), ttl, timeunit);
    }
  }

  public static class HCMultiMapImpl<V> implements MultiMap<V> {
    private com.hazelcast.core.MultiMap<String, HCDrillSerializableImpl> mmap;
    private Class<V> clazz;

    public HCMultiMapImpl(com.hazelcast.core.MultiMap mmap, Class<V> clazz) {
      this.mmap = mmap;
      this.clazz = clazz;
    }

    public Collection<DrillSerializable> get(String key) {
      List<DrillSerializable> list = Lists.newArrayList();
      for (HCDrillSerializableImpl v : mmap.get(key)) {
        list.add(v.get());
      }
      return list;
    }

    @Override
    public void put(String key, DrillSerializable value) {
      mmap.put(key, HCDrillSerializableImpl.getWrapper(value, clazz));
    }
  }

  public static class HCCounterImpl implements Counter {
    private AtomicNumber n;

    public HCCounterImpl(AtomicNumber n) {
      this.n = n;
    }

    public long get() {
      return n.get();
    }

    public long incrementAndGet() {
      return n.incrementAndGet();
    }

    public long decrementAndGet() {
      return n.decrementAndGet();
    }
  }

}
