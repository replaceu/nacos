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

package com.alibaba.nacos.core.distributed.distro;

import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.distributed.distro.component.DistroCallback;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataProcessor;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataStorage;
import com.alibaba.nacos.core.distributed.distro.component.DistroTransportAgent;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.entity.DistroKey;
import com.alibaba.nacos.core.distributed.distro.task.DistroTaskEngineHolder;
import com.alibaba.nacos.core.distributed.distro.task.delay.DistroDelayTask;
import com.alibaba.nacos.core.distributed.distro.task.load.DistroLoadDataTask;
import com.alibaba.nacos.core.distributed.distro.task.verify.DistroVerifyTask;
import com.alibaba.nacos.core.utils.GlobalExecutor;
import com.alibaba.nacos.core.utils.Loggers;
import com.alibaba.nacos.sys.env.EnvUtil;
import org.springframework.stereotype.Component;

/**
 * Distro protocol.
 *
 * @author xiweng.yy
 */
@Component
public class DistroProtocol {

	//服务器成员管理器
	private final ServerMemberManager memberManager;

	//distro组件的持有者，用来注册/获取各个组件
	private final DistroComponentHolder distroComponentHolder;

	//distro任务引擎持有者
	private final DistroTaskEngineHolder distroTaskEngineHolder;

	private final DistroConfig distroConfig;

	private volatile boolean isInitialized = false;

	/**
	 * 如果nacos集群中有新的节点加入，那么新节点就会从其他节点进行全量拉取数据。
	 * 当DistroProtocol初始化时，调用startDistroTask方法进行全量拉取数据
	 * @param memberManager
	 * @param distroComponentHolder
	 * @param distroTaskEngineHolder
	 * @param distroConfig
	 */
	public DistroProtocol(ServerMemberManager memberManager, DistroComponentHolder distroComponentHolder, DistroTaskEngineHolder distroTaskEngineHolder, DistroConfig distroConfig) {
		this.memberManager = memberManager;
		this.distroComponentHolder = distroComponentHolder;
		this.distroTaskEngineHolder = distroTaskEngineHolder;
		this.distroConfig = distroConfig;
		startDistroTask();
	}

	private void startDistroTask() {
		if (EnvUtil.getStandaloneMode()) {
			isInitialized = true;
			return;
		}
		//开启验证任务
		startVerifyTask();
		//开始全量拉取数据
		startLoadTask();
	}

	private void startLoadTask() {
		DistroCallback loadCallback = new DistroCallback() {
			@Override
			public void onSuccess() {
				isInitialized = true;
			}
			@Override
			public void onFailed(Throwable throwable) {
				isInitialized = false;
			}
		};
		//提交全量拉取数据的任务
		GlobalExecutor.submitLoadDataTask(new DistroLoadDataTask(memberManager, distroComponentHolder, distroConfig, loadCallback));
	}

	private void startVerifyTask() {
		GlobalExecutor.schedulePartitionDataTimedSync(new DistroVerifyTask(memberManager, distroComponentHolder), distroConfig.getVerifyIntervalMillis());
	}

	public boolean isInitialized() {
		return isInitialized;
	}

	/**
	 * Start to sync by configured delay.
	 *
	 * @param distroKey distro key of sync data
	 * @param action    the action of data operation
	 */
	public void sync(DistroKey distroKey, DataOperation action) {
		sync(distroKey, action, distroConfig.getSyncDelayMillis());
	}

	/**
	 * Start to sync data to all remote server
	 * Nacos 同步临时实例数据到集群其他节点
	 *
	 * @param distroKey distro key of sync data
	 * @param action    the action of data operation
	 */
	public void sync(DistroKey distroKey, DataOperation action, long delay) {
        //遍历除了自身的所有集群节点
		for (Member each : memberManager.allMembersWithoutSelf()) {
			DistroKey distroKeyWithTarget = new DistroKey(distroKey.getResourceKey(), distroKey.getResourceType(), each.getAddress());
            //封装为 Distro延迟任务
			DistroDelayTask distroDelayTask = new DistroDelayTask(distroKeyWithTarget, action, delay);
            //获取延迟任务执行引擎，并将Distro延迟任务添加到延迟任务执行引擎中
			distroTaskEngineHolder.getDelayTaskExecuteEngine().addTask(distroKeyWithTarget, distroDelayTask);
			if (Loggers.DISTRO.isDebugEnabled()) {
				Loggers.DISTRO.debug("[DISTRO-SCHEDULE] {} to {}", distroKey, each.getAddress());
			}
		}
	}

	/**
	 * Query data from specified server.
	 *
	 * @param distroKey data key
	 * @return data
	 */
	public DistroData queryFromRemote(DistroKey distroKey) {
		if (null == distroKey.getTargetServer()) {
			Loggers.DISTRO.warn("[DISTRO] Can't query data from empty server");
			return null;
		}
		String resourceType = distroKey.getResourceType();
		DistroTransportAgent transportAgent = distroComponentHolder.findTransportAgent(resourceType);
		if (null == transportAgent) {
			Loggers.DISTRO.warn("[DISTRO] Can't find transport agent for key {}", resourceType);
			return null;
		}
		return transportAgent.getData(distroKey, distroKey.getTargetServer());
	}

	/**
	 * Receive synced distro data, find processor to process.
	 *
	 * @param distroData Received data
	 * @return true if handle receive data successfully, otherwise false
	 */
	public boolean onReceive(DistroData distroData) {
		String resourceType = distroData.getDistroKey().getResourceType();
		//获取处理器
		DistroDataProcessor dataProcessor = distroComponentHolder.findDataProcessor(resourceType);
		if (null == dataProcessor) {
			Loggers.DISTRO.warn("[DISTRO] Can't find data process for received data {}", resourceType);
			return false;
		}
		//处理同步的实例数据
		return dataProcessor.processData(distroData);
	}

	/**
	 * Receive verify data, find processor to process.
	 *
	 * @param distroData verify data
	 * @return true if verify data successfully, otherwise false
	 */
	public boolean onVerify(DistroData distroData) {
		String resourceType = distroData.getDistroKey().getResourceType();
		DistroDataProcessor dataProcessor = distroComponentHolder.findDataProcessor(resourceType);
		if (null == dataProcessor) {
			Loggers.DISTRO.warn("[DISTRO] Can't find verify data process for received data {}", resourceType);
			return false;
		}
		return dataProcessor.processVerifyData(distroData);
	}

	/**
	 * Query data of input distro key.
	 *
	 * @param distroKey key of data
	 * @return data
	 */
	public DistroData onQuery(DistroKey distroKey) {
		String resourceType = distroKey.getResourceType();
		DistroDataStorage distroDataStorage = distroComponentHolder.findDataStorage(resourceType);
		if (null == distroDataStorage) {
			Loggers.DISTRO.warn("[DISTRO] Can't find data storage for received key {}", resourceType);
			return new DistroData(distroKey, new byte[0]);
		}
		return distroDataStorage.getDistroData(distroKey);
	}

	/**
	 * Query all datum snapshot.
	 *
	 * @param type datum type
	 * @return all datum snapshot
	 */
	public DistroData onSnapshot(String type) {
		//根据类型获取数据存储器
		DistroDataStorage distroDataStorage = distroComponentHolder.findDataStorage(type);
		if (null == distroDataStorage) {
			Loggers.DISTRO.warn("[DISTRO] Can't find data storage for received key {}", type);
			return new DistroData(new DistroKey("snapshot", type), new byte[0]);
		}
		return distroDataStorage.getDatumSnapshot();
	}
}
