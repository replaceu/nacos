/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.common.task.engine;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.executor.ExecutorFactory;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.task.AbstractDelayTask;
import com.alibaba.nacos.common.task.NacosTaskProcessor;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Nacos delay task execute engine.
 *
 * @author xiweng.yy
 */
//延迟任务执行引擎
public class NacosDelayTaskExecuteEngine extends AbstractNacosTaskExecuteEngine<AbstractDelayTask> {
    //任务调度线程
    private final ScheduledExecutorService processingExecutor;
    //tasks是用来保存延迟任务的
    protected final ConcurrentHashMap<Object, AbstractDelayTask> tasks;
    
    protected final ReentrantLock lock = new ReentrantLock();
    
    public NacosDelayTaskExecuteEngine(String name) {
        this(name, null);
    }
    
    public NacosDelayTaskExecuteEngine(String name, Logger logger) {
        this(name, 32, logger, 100L);
    }
    
    public NacosDelayTaskExecuteEngine(String name, Logger logger, long processInterval) {
        this(name, 32, logger, processInterval);
    }
    
    public NacosDelayTaskExecuteEngine(String name, int initCapacity, Logger logger) {
        this(name, initCapacity, logger, 100L);
    }
    
    public NacosDelayTaskExecuteEngine(String name, int initCapacity, Logger logger, long processInterval) {
        super(logger);
        tasks = new ConcurrentHashMap<Object, AbstractDelayTask>(initCapacity);
        //todo:当初始化NacosDelayTaskExecuteEngine时，会初始化processingExecutor，并进行延迟任务的处理
        processingExecutor = ExecutorFactory.newSingleScheduledExecutorService(new NameThreadFactory(name));
        processingExecutor.scheduleWithFixedDelay(new ProcessRunnable(), processInterval, processInterval, TimeUnit.MILLISECONDS);
    }
    
    @Override
    public int size() {
        lock.lock();
        try {
            return tasks.size();
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public boolean isEmpty() {
        lock.lock();
        try {
            return tasks.isEmpty();
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public AbstractDelayTask removeTask(Object key) {
        lock.lock();
        try {
            AbstractDelayTask task = tasks.get(key);
            if (null != task && task.shouldProcess()) {
                return tasks.remove(key);
            } else {
                return null;
            }
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public Collection<Object> getAllTaskKeys() {
        Collection<Object> keys = new HashSet<Object>();
        lock.lock();
        try {
            keys.addAll(tasks.keySet());
        } finally {
            lock.unlock();
        }
        return keys;
    }
    
    @Override
    public void shutdown() throws NacosException {
        processingExecutor.shutdown();
    }
    
    @Override
    public void addTask(Object key, AbstractDelayTask newTask) {
        lock.lock();
        try {
            //根据key获取延迟任务
            AbstractDelayTask existTask = tasks.get(key);
            if (null != existTask) {
                //如果延迟任务存在，则进行合并
                newTask.merge(existTask);
            }
            //如果延迟任务不存在，则添加到tasks
            tasks.put(key, newTask);
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * process tasks in execute engine.在执行引擎中处理任务
     */
    protected void processTasks() {
        //获取所有的任务key，并且进行遍历
        Collection<Object> keys = getAllTaskKeys();
        for (Object taskKey : keys) {
            //从tasks容器中根据key删除task
            AbstractDelayTask task = removeTask(taskKey);
            //如果key不存在，则继续
            if (null == task) {
                continue;
            }
            //todo：根据taskKey获取任务处理器，如果处理器为null，则继续
            NacosTaskProcessor processor = getProcessor(taskKey);
            if (null == processor) {
                getEngineLog().error("processor not found for task, so discarded. " + task);
                continue;
            }
            try {
                //todo：处理器处理任务，并依据返回结果进行是否重试判断
                boolean isProcess = processor.process(task);
                if (!isProcess) {
                    //调用处理的process方法，如果处理失败，则进行重试
                    retryFailedTask(taskKey, task);
                }
            } catch (Throwable e) {
                getEngineLog().error("Nacos task execute error : " + e.toString(), e);
                //发生异常也进行任务重试
                retryFailedTask(taskKey, task);
            }
        }
    }

    //失败任务的重试
    private void retryFailedTask(Object key, AbstractDelayTask task) {
        //设置任务的处理时间，并添加任务到tasks容器中
        task.setLastProcessTime(System.currentTimeMillis());
        //tasks是用来保存延迟任务的。addTask方法就是往tasks添加任务的
        addTask(key, task);
    }
    
    private class ProcessRunnable implements Runnable {
        @Override
        public void run() {
            try {
                processTasks();
            } catch (Throwable e) {
                getEngineLog().error(e.toString(), e);
            }
        }
    }
}
