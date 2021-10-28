/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.transport;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueDatagramChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Factory class to return the native transport for Netty.
 * Native transport (supported on Linux and Mac) is slightly more performing
 * than the default nio transport. So use it if applicable.
 *
 * Use system property io.netty.transport.noNative = false to fallback to
 * the default nio transport.
 *
 * More details is available at https://netty.io/wiki/native-transports.html
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NettyTransportFactory {
  public static final Logger LOG =
      LoggerFactory.getLogger(NettyTransportFactory.class);

  public static EventLoopGroup createBossGroup() {
    ThreadFactory namedThreadFactory =
        new ThreadFactoryBuilder().setNameFormat("boss-%d").build();

    if (Epoll.isAvailable()) {
      LOG.debug("server uses epoll transport");
      return new EpollEventLoopGroup(namedThreadFactory);
    }

    if (KQueue.isAvailable()) {
      LOG.debug("server uses kqueue transport");
      return new KQueueEventLoopGroup(namedThreadFactory);
    }

    LOG.debug("server uses nio transport");
    return new NioEventLoopGroup(namedThreadFactory);
  }

  /**
   * Return a native event loop group implementation if applicable.
   * Return the default NIO implementation if none is available.
   * @return
   */
  public static EventLoopGroup createWorkerGroup(int workerCount) {
    // use native transport whenever possible:
    // epoll for Linux and kqueue for Mac
    if (Epoll.isAvailable()) {
      return new EpollEventLoopGroup(workerCount, Executors.newCachedThreadPool());
    }

    if (KQueue.isAvailable()) {
      return new KQueueEventLoopGroup(workerCount, Executors.newCachedThreadPool());
    }

    return new NioEventLoopGroup(workerCount, Executors.newCachedThreadPool());
  }

  /**
   * Return a native socket channel implementation if applicable.
   * Return the default NIO implementation if none is available.
   * @return
   */
  public static Class<? extends SocketChannel> getSocketChannelClass() {
    if (Epoll.isAvailable()) {
      return EpollSocketChannel.class;
    }

    if (KQueue.isAvailable()) {
      return KQueueSocketChannel.class;
    }

    return NioSocketChannel.class;
  }

  /**
   * Return a native server socket channel implementation if applicable.
   * Return the default NIO implementation if none is available.
   * @return
   */
  public static Class<? extends ServerChannel> getServerSocketChannelClass() {

    if (Epoll.isAvailable()) {
      return EpollServerSocketChannel.class;
    }

    if (KQueue.isAvailable()) {
      return KQueueServerSocketChannel.class;
    }

    return NioServerSocketChannel.class;
  }

  public static Class<? extends DatagramChannel> getDatagramChannel() {

    if (Epoll.isAvailable()) {
      return EpollDatagramChannel.class;
    }

    if (KQueue.isAvailable()) {
      return KQueueDatagramChannel.class;
    }

    return NioDatagramChannel.class;
  }
}
