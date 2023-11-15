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

import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.task.NacosTask;
import com.alibaba.nacos.common.task.NacosTaskProcessor;

import java.util.Collection;

/**
 * Nacos task execute engine.
 *
 * @author xiweng.yy
 */
public interface NacosTaskExecuteEngine<T extends NacosTask> extends Closeable {
    
    /**
     * task的数量
     * @return size of task
     */
    int size();
    
    /**
     * 执行引擎是否为空
     * @return true if the execute engine has no task to do, otherwise false
     */
    boolean isEmpty();
    
    /**
     * Add task processor {@link NacosTaskProcessor} for execute engine.
     * 添加任务处理器
     * @param key           key of task
     * @param taskProcessor task processor
     */
    void addProcessor(Object key, NacosTaskProcessor taskProcessor);
    
    /**
     * Remove task processor {@link NacosTaskProcessor} form execute engine for key.
     * 根据key移除任务处理器
     * @param key key of task
     */
    void removeProcessor(Object key);
    
    /**
     * Try to get {@link NacosTaskProcessor} by key, if non-exist, will return default processor.
     * 根据key获取任务处理器
     * @param key key of task
     * @return task processor for task key or default processor if task processor for task key non-exist
     */
    NacosTaskProcessor getProcessor(Object key);
    
    /**
     * Get all processor key.
     * 获取所有的任务处理器
     * @return collection of processors
     */
    Collection<Object> getAllProcessorKey();
    
    /**
     * 设置默认的任务处理器
     * @param defaultTaskProcessor default task processor
     */
    void setDefaultTaskProcessor(NacosTaskProcessor defaultTaskProcessor);
    
    /**
     * Add task into execute pool.
     * 根据key添加任务
     * @param key  key of task
     * @param task task
     */
    void addTask(Object key, T task);
    
    /**
     * Remove task.
     * 根据key删除任务
     * @param key key of task
     * @return nacos task
     */
    T removeTask(Object key);
    
    /**
     * Get all task keys.
     * 获取所有任务的key
     * @return collection of task keys.
     */
    Collection<Object> getAllTaskKeys();
}
