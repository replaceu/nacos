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

package com.alibaba.nacos.core.distributed.distro.task;

import com.alibaba.nacos.common.task.NacosTaskProcessor;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.task.delay.DistroDelayTaskExecuteEngine;
import com.alibaba.nacos.core.distributed.distro.task.delay.DistroDelayTaskProcessor;
import com.alibaba.nacos.core.distributed.distro.task.execute.DistroExecuteTaskExecuteEngine;
import org.springframework.stereotype.Component;

/**
 * Distro task engine holder.
 *
 * @author xiweng.yy
 */
@Component
public class DistroTaskEngineHolder {
    //延迟任务执行引擎
    private final DistroDelayTaskExecuteEngine delayTaskExecuteEngine = new DistroDelayTaskExecuteEngine();
    //普通任务执行引擎
    private final DistroExecuteTaskExecuteEngine executeWorkersManager = new DistroExecuteTaskExecuteEngine();
    
    public DistroTaskEngineHolder(DistroComponentHolder distroComponentHolder) {
        //设置默认的延迟任务处理器
        DistroDelayTaskProcessor defaultDelayTaskProcessor = new DistroDelayTaskProcessor(this, distroComponentHolder);
        delayTaskExecuteEngine.setDefaultTaskProcessor(defaultDelayTaskProcessor);
    }
    
    public DistroDelayTaskExecuteEngine getDelayTaskExecuteEngine() {
        return delayTaskExecuteEngine;
    }
    
    public DistroExecuteTaskExecuteEngine getExecuteWorkersManager() {
        return executeWorkersManager;
    }
    //延迟任务执行引擎注册处理器
    public void registerNacosTaskProcessor(Object key, NacosTaskProcessor nacosTaskProcessor) {
        this.delayTaskExecuteEngine.addProcessor(key, nacosTaskProcessor);
    }
}
