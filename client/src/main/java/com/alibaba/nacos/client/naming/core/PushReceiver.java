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

package com.alibaba.nacos.client.naming.core;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.utils.IoUtils;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Push receiver.
 *
 * @author xuanyin
 */
public class PushReceiver implements Runnable, Closeable {

	private static final Charset UTF_8 = Charset.forName("UTF-8");

	private static final int UDP_MSS = 64 * 1024;

	private ScheduledExecutorService executorService;

	private DatagramSocket udpSocket;

	private HostReactor hostReactor;

	private volatile boolean closed = false;

	public static String getPushReceiverUdpPort() {
		return System.getenv(PropertyKeyConst.PUSH_RECEIVER_UDP_PORT);
	}

	public PushReceiver(HostReactor hostReactor) {
		try {
			this.hostReactor = hostReactor;
			//获取udp端口，从环境变量中获取push.receiver.udp.port的值
			String udpPort = getPushReceiverUdpPort();
			if (StringUtils.isEmpty(udpPort)) {
				this.udpSocket = new DatagramSocket();
			} else {
				this.udpSocket = new DatagramSocket(new InetSocketAddress(Integer.parseInt(udpPort)));
			}
			//初始化线程池，用来调度该对象
			this.executorService = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					Thread thread = new Thread(r);
					thread.setDaemon(true);
					thread.setName("com.alibaba.nacos.naming.push.receiver");
					return thread;
				}
			});

			this.executorService.execute(this);
		} catch (Exception e) {
			NAMING_LOGGER.error("[NA] init udp socket failed", e);
		}
	}

	@Override
	public void run() {
		while (!closed) {
			try {
				//buffer作为接收到数据的缓冲池
				// byte[] is initialized with 0 full filled by default
				byte[] buffer = new byte[UDP_MSS];
				DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
				//todo:接收数据
				udpSocket.receive(packet);

				String json = new String(IoUtils.tryDecompress(packet.getData()), UTF_8).trim();
				NAMING_LOGGER.info("received push data: " + json + " from " + packet.getAddress().toString());
				//将接收到的数据，转换为PushPacket队列
				PushPacket pushPacket = JacksonUtils.toObj(json, PushPacket.class);
				String ack;
				//拼接ack响应
				if ("dom".equals(pushPacket.type) || "service".equals(pushPacket.type)) {
					//处理数据包
					hostReactor.processServiceJson(pushPacket.data);
					// send ack to server
					ack = "{\"type\": \"push-ack\"" + ", \"lastRefTime\":\"" + pushPacket.lastRefTime + "\", \"data\":" + "\"\"}";
				} else if ("dump".equals(pushPacket.type)) {
					// dump data to server
					ack = "{\"type\": \"dump-ack\"" + ", \"lastRefTime\": \"" + pushPacket.lastRefTime + "\", \"data\":" + "\"" + StringUtils.escapeJavaScript(JacksonUtils.toJson(hostReactor.getServiceInfoMap())) + "\"}";
				} else {
					// do nothing send ack only
					ack = "{\"type\": \"unknown-ack\"" + ", \"lastRefTime\":\"" + pushPacket.lastRefTime + "\", \"data\":" + "\"\"}";
				}
				//todo:发送ack响应给nacos服务端
				udpSocket.send(new DatagramPacket(ack.getBytes(UTF_8), ack.getBytes(UTF_8).length, packet.getSocketAddress()));
			} catch (Exception e) {
				if (closed) { return; }
				NAMING_LOGGER.error("[NA] error while receiving push data", e);
			}
		}
	}

	@Override
	public void shutdown() throws NacosException {
		String className = this.getClass().getName();
		NAMING_LOGGER.info("{} do shutdown begin", className);
		ThreadUtils.shutdownThreadPool(executorService, NAMING_LOGGER);
		closed = true;
		udpSocket.close();
		NAMING_LOGGER.info("{} do shutdown stop", className);
	}

	public static class PushPacket {

		public String type;

		public long lastRefTime;

		public String data;
	}

	public int getUdpPort() {
		return this.udpSocket.getLocalPort();
	}
}
