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

package com.alibaba.nacos.naming.consistency.ephemeral.distro;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.utils.Objects;
import com.alibaba.nacos.naming.cluster.ServerStatus;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.ephemeral.EphemeralConsistencyService;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.combined.DistroHttpCombinedKey;
import com.alibaba.nacos.core.distributed.distro.DistroProtocol;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataProcessor;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.entity.DistroKey;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.pojo.Record;
import org.apache.commons.lang3.StringUtils;
import org.javatuples.Pair;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A consistency protocol algorithm called <b>Distro</b>
 *
 * <p>Use a distro algorithm to divide data into many blocks. Each Nacos server node takes responsibility for exactly
 * one block of data. Each block of data is generated, removed and synchronized by its responsible server. So every
 * Nacos server only handles writings for a subset of the total service data.
 *
 * <p>At mean time every Nacos server receives data sync of other Nacos server, so every Nacos server will eventually
 * have a complete set of data.
 *
 * @author nkorange
 * @since 1.0.0
 */
@DependsOn("ProtocolManager")
@org.springframework.stereotype.Service("distroConsistencyService")
public class DistroConsistencyServiceImpl implements EphemeralConsistencyService, DistroDataProcessor {

	private final DistroMapper distroMapper;

	private final DataStore dataStore;

	private final Serializer serializer;

	private final SwitchDomain switchDomain;

	private final GlobalConfig globalConfig;

	private final DistroProtocol distroProtocol;

	private volatile Notifier notifier = new Notifier();

	private Map<String, ConcurrentLinkedQueue<RecordListener>> listeners = new ConcurrentHashMap<>();

	private Map<String, String> syncChecksumTasks = new ConcurrentHashMap<>(16);

	public DistroConsistencyServiceImpl(DistroMapper distroMapper, DataStore dataStore, Serializer serializer, SwitchDomain switchDomain, GlobalConfig globalConfig, DistroProtocol distroProtocol) {
		this.distroMapper = distroMapper;
		this.dataStore = dataStore;
		this.serializer = serializer;
		this.switchDomain = switchDomain;
		this.globalConfig = globalConfig;
		this.distroProtocol = distroProtocol;
	}

	@PostConstruct
	public void init() {
		GlobalExecutor.submitDistroNotifyTask(notifier);
	}

	@Override
	public void put(String key, Record value) throws NacosException {
		//保存instance到内存，并添加服务变更通知到队列中
		onPut(key, value);
		//集群间的数据一致性同步
		distroProtocol.sync(new DistroKey(key, KeyBuilder.INSTANCE_LIST_KEY_PREFIX), DataOperation.CHANGE, globalConfig.getTaskDispatchPeriod() / 2);
	}

	@Override
	public void remove(String key) throws NacosException {
		onRemove(key);
		listeners.remove(key);
	}

	@Override
	public Datum get(String key) throws NacosException {
		return dataStore.get(key);
	}

	/**
	 * Put a new record.
	 *
	 * @param key   key of record
	 * @param value record
	 */
	public void onPut(String key, Record value) {
		//如果instance是临时的，将保存到内存中
		if (KeyBuilder.matchEphemeralInstanceListKey(key)) {
			Datum<Instances> datum = new Datum<>();
			datum.value = (Instances) value;
			datum.key = key;
			datum.timestamp.incrementAndGet();
			dataStore.put(key, datum);
		}
		//如果监听器没有包含key，则直接返回
		if (!listeners.containsKey(key)) { return; }
		//添加数据变更任务到队列中
		notifier.addTask(key, DataOperation.CHANGE);
	}

	/**
	 * Remove a record.
	 *
	 * @param key key of record
	 */
	public void onRemove(String key) {
		dataStore.remove(key);
		if (!listeners.containsKey(key)) { return; }
		notifier.addTask(key, DataOperation.DELETE);
	}

	/**
	 * Check sum when receive checksums request.
	 *
	 * @param checksumMap map of checksum
	 * @param server      source server request checksum
	 */
	public void onReceiveChecksums(Map<String, String> checksumMap, String server) {
		if (syncChecksumTasks.containsKey(server)) {
			// Already in process of this server:
			Loggers.DISTRO.warn("sync checksum task already in process with {}", server);
			return;
		}
		syncChecksumTasks.put(server, "1");
		try {
			List<String> toUpdateKeys = new ArrayList<>();
			List<String> toRemoveKeys = new ArrayList<>();
			for (Map.Entry<String, String> entry : checksumMap.entrySet()) {
				if (distroMapper.responsible(KeyBuilder.getServiceName(entry.getKey()))) {
					// this key should not be sent from remote server:
					Loggers.DISTRO.error("receive responsible key timestamp of " + entry.getKey() + " from " + server);
					// abort the procedure:
					return;
				}

				if (!dataStore.contains(entry.getKey()) || dataStore.get(entry.getKey()).value == null || !dataStore.get(entry.getKey()).value.getChecksum().equals(entry.getValue())) {
					toUpdateKeys.add(entry.getKey());
				}
			}
			for (String key : dataStore.keys()) {
				if (!server.equals(distroMapper.mapSrv(KeyBuilder.getServiceName(key)))) {
					continue;
				}

				if (!checksumMap.containsKey(key)) {
					toRemoveKeys.add(key);
				}
			}
			Loggers.DISTRO.info("to remove keys: {}, to update keys: {}, source: {}", toRemoveKeys, toUpdateKeys, server);
			for (String key : toRemoveKeys) {
				onRemove(key);
			}
			if (toUpdateKeys.isEmpty()) { return; }
			try {
				DistroHttpCombinedKey distroKey = new DistroHttpCombinedKey(KeyBuilder.INSTANCE_LIST_KEY_PREFIX, server);
				distroKey.getActualResourceTypes().addAll(toUpdateKeys);
				DistroData remoteData = distroProtocol.queryFromRemote(distroKey);
				if (null != remoteData) {
					processData(remoteData.getContent());
				}
			} catch (Exception e) {
				Loggers.DISTRO.error("get data from " + server + " failed!", e);
			}
		} finally {
			// Remove this 'in process' flag:
			syncChecksumTasks.remove(server);
		}
	}

	private boolean processData(byte[] data) throws Exception {
		//如果data的数据长度为0
		if (data.length > 0) {
			//对拉取的数据进行反序列化
			Map<String, Datum<Instances>> datumMap = serializer.deserializeMap(data, Instances.class);
			//对拉取回来的数据进行反序列化
			for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {
				//添加到内存中
				dataStore.put(entry.getKey(), entry.getValue());
				//如果监听器没有包含该服务
				if (!listeners.containsKey(entry.getKey())) {
					// pretty sure the service not exist:
					if (switchDomain.isDefaultInstanceEphemeral()) {
						// create empty service
						//创建新的service对象
						Loggers.DISTRO.info("creating service {}", entry.getKey());
						Service service = new Service();
						String serviceName = KeyBuilder.getServiceName(entry.getKey());
						String namespaceId = KeyBuilder.getNamespace(entry.getKey());
						service.setName(serviceName);
						service.setNamespaceId(namespaceId);
						service.setGroupName(Constants.DEFAULT_GROUP);
						// now validate the service. if failed, exception will be thrown
						service.setLastModifiedMillis(System.currentTimeMillis());
						service.recalculateChecksum();
						// The Listener corresponding to the key value must not be empty
						RecordListener listener = listeners.get(KeyBuilder.SERVICE_META_KEY_PREFIX).peek();
						if (Objects.isNull(listener)) { return false; }
						listener.onChange(KeyBuilder.buildServiceMetaKey(namespaceId, serviceName), service);
					}
				}
			}
			//遍历所有的数据，并通知监听器
			for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {
				if (!listeners.containsKey(entry.getKey())) {
					// Should not happen:
					Loggers.DISTRO.warn("listener of {} not found.", entry.getKey());
					continue;
				}
				try {
					for (RecordListener listener : listeners.get(entry.getKey())) {
						listener.onChange(entry.getKey(), entry.getValue().value);
					}
				} catch (Exception e) {
					Loggers.DISTRO.error("[NACOS-DISTRO] error while execute listener of key: {}", entry.getKey(), e);
					continue;
				}

				// Update data store if listener executed successfully:
				dataStore.put(entry.getKey(), entry.getValue());
			}
		}
		return true;
	}

	@Override
	public boolean processData(DistroData distroData) {
		DistroHttpData distroHttpData = (DistroHttpData) distroData;
		//进行反序列化
		Datum<Instances> datum = (Datum<Instances>) distroHttpData.getDeserializedContent();
		//将临时数据缓存到内容并进行通知
		onPut(datum.key, datum.value);
		return true;
	}

	@Override
	public String processType() {
		return KeyBuilder.INSTANCE_LIST_KEY_PREFIX;
	}

	@Override
	public boolean processVerifyData(DistroData distroData) {
		DistroHttpData distroHttpData = (DistroHttpData) distroData;
		String sourceServer = distroData.getDistroKey().getResourceKey();
		Map<String, String> verifyData = (Map<String, String>) distroHttpData.getDeserializedContent();
		onReceiveChecksums(verifyData, sourceServer);
		return true;
	}

	@Override
	public boolean processSnapshot(DistroData distroData) {
		try {
			return processData(distroData.getContent());
		} catch (Exception e) {
			return false;
		}
	}

	@Override
	public void listen(String key, RecordListener listener) throws NacosException {
		if (!listeners.containsKey(key)) {
			listeners.put(key, new ConcurrentLinkedQueue<>());
		}

		if (listeners.get(key).contains(listener)) { return; }

		listeners.get(key).add(listener);
	}

	@Override
	public void unListen(String key, RecordListener listener) throws NacosException {
		if (!listeners.containsKey(key)) { return; }
		for (RecordListener recordListener : listeners.get(key)) {
			if (recordListener.equals(listener)) {
				listeners.get(key).remove(listener);
				break;
			}
		}
	}

	@Override
	public boolean isAvailable() {
		return isInitialized() || ServerStatus.UP.name().equals(switchDomain.getOverriddenServerStatus());
	}

	@Override
	public Optional<String> getErrorMsg() {
		String errorMsg;
		if (!isInitialized() && !ServerStatus.UP.name().equals(switchDomain.getOverriddenServerStatus())) {
			errorMsg = "Distro protocol is not initialized";
		} else {
			errorMsg = null;
		}
		return Optional.ofNullable(errorMsg);
	}

	public boolean isInitialized() {
		return distroProtocol.isInitialized() || !globalConfig.isDataWarmup();
	}

	public class Notifier implements Runnable {

		private ConcurrentHashMap<String, String> services = new ConcurrentHashMap<>(10 * 1024);
		private BlockingQueue<Pair<String, DataOperation>> tasks = new ArrayBlockingQueue<>(1024 * 1024);

		/**
		 * Add new notify task to queue.
		 *
		 * @param datumKey data key
		 * @param action   action for data
		 */
		public void addTask(String datumKey, DataOperation action) {
			if (services.containsKey(datumKey) && action == DataOperation.CHANGE) { return; }
			if (action == DataOperation.CHANGE) {
				services.put(datumKey, StringUtils.EMPTY);
			}
			tasks.offer(Pair.with(datumKey, action));
		}

		public int getTaskSize() {
			return tasks.size();
		}

		/**
		 * run不断从tasks队列中拿出服务变更的key，交给handle方法处理
		 */
		@Override
		public void run() {
			Loggers.DISTRO.info("distro notifier started");

			for (;;) {
				try {
					Pair<String, DataOperation> pair = tasks.take();
					handle(pair);
				} catch (Throwable e) {
					Loggers.DISTRO.error("[NACOS-DISTRO] Error while handling notifying task", e);
				}
			}
		}

		private void handle(Pair<String, DataOperation> pair) {
			try {
				String datumKey = pair.getValue0();
				DataOperation action = pair.getValue1();
				services.remove(datumKey);
				int count = 0;
				if (!listeners.containsKey(datumKey)) { return; }
				//遍历监听器
				for (RecordListener listener : listeners.get(datumKey)) {
					count++;
					try {
						//如果属于服务变更，则调用监听的onChange方法
						if (action == DataOperation.CHANGE) {
							listener.onChange(datumKey, dataStore.get(datumKey).value);
							continue;
						}
						if (action == DataOperation.DELETE) {
							listener.onDelete(datumKey);
							continue;
						}
					} catch (Throwable e) {
						Loggers.DISTRO.error("[NACOS-DISTRO] error while notifying listener of key: {}", datumKey, e);
					}
				}

				if (Loggers.DISTRO.isDebugEnabled()) {
					Loggers.DISTRO.debug("[NACOS-DISTRO] datum change notified, key: {}, listener count: {}, action: {}", datumKey, count, action.name());
				}
			} catch (Throwable e) {
				Loggers.DISTRO.error("[NACOS-DISTRO] Error while handling notifying task", e);
			}
		}
	}
}
