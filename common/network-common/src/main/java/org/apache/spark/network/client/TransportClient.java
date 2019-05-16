/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.protocol.ChunkFetchRequest;
import org.apache.spark.network.protocol.OneWayMessage;
import org.apache.spark.network.protocol.RpcRequest;
import org.apache.spark.network.protocol.StreamChunkId;
import org.apache.spark.network.protocol.StreamRequest;
import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

/**
 *
 * =====>>创建TransportClient
 * 客户端用于获取预先协商的流的连续块。此API旨在允许*有效传输大量数据，分解为大小从几百KB到几MB的块。
 * 请注意，虽然此客户端处理从流（即数据平面）获取块的情况，但是流的实际设置是在传输层的范围之外完成的。提供方便方法
 * “sendRPC”以使客户端和服务器之间的控制平面通信能够执行该设置。
 * 例如，典型的工作流程可能是：
 *  client.sendRPC（new OpenFile（“/ foo”）） - ＆gt;返回StreamId = 100
 *  client.fetchChunk（streamId = 100，chunkIndex = 0，回调）
 *  client.fetchChunk（streamId = 100，chunkIndex = 1，回调）
 *  ...  client.sendRPC（new CloseStream（100）
 *使用{@link TransportClientFactory}构造TransportClient的实例。单个TransportClient可以用于多个流，
 * 但是任何给定的流必须限制为*单个客户端，以避免无序响应。
 * 注意：此类用于向服务器发出请求，而{@link TransportResponseHandler}负责处理来自服务器的响应。
 * 并发：线程安全，可以从多个线程调用。
 */
public class TransportClient implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(TransportClient.class);

  private final Channel channel;
  private final TransportResponseHandler handler;
  @Nullable private String clientId;
  private volatile boolean timedOut;

  public TransportClient(Channel channel, TransportResponseHandler handler) {
    this.channel = Preconditions.checkNotNull(channel);
    this.handler = Preconditions.checkNotNull(handler);
    this.timedOut = false;
  }

  public Channel getChannel() {
    return channel;
  }

  public boolean isActive() {
    return !timedOut && (channel.isOpen() || channel.isActive());
  }

  public SocketAddress getSocketAddress() {
    return channel.remoteAddress();
  }

  /**
   *
   * 返回启用身份验证时客户端用于对自身进行身份验证的ID。
   *
   * @return 客户端ID，如果禁用身份验证，则为null。
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * 设置经过身份验证的客户端ID。这意味着由身份验证层使用。
   * 在设置后尝试设置不同的客户端ID将导致异常。
   */
  public void setClientId(String id) {
    Preconditions.checkState(clientId == null, "Client ID has already been set.");
    this.clientId = id;
  }

  /**
   * Requests a single chunk from the remote side, from the pre-negotiated streamId.
   *
   * Chunk indices go from 0 onwards. It is valid to request the same chunk multiple times, though
   * some streams may not support this.
   *
   * Multiple fetchChunk requests may be outstanding simultaneously, and the chunks are guaranteed
   * to be returned in the same order that they were requested, assuming only a single
   * TransportClient is used to fetch the chunks.
   *
   * @param streamId Identifier that refers to a stream in the remote StreamManager. This should
   *                 be agreed upon by client and server beforehand.
   * @param chunkIndex 0-based index of the chunk to fetch
   * @param callback Callback invoked upon successful receipt of chunk, or upon any failure.
   */
  public void fetchChunk(
      long streamId,
      int chunkIndex,
      ChunkReceivedCallback callback) {
    long startTime = System.currentTimeMillis();
    if (logger.isDebugEnabled()) {
      logger.debug("Sending fetch chunk request {} to {}", chunkIndex, getRemoteAddress(channel));
    }

    StreamChunkId streamChunkId = new StreamChunkId(streamId, chunkIndex);
    handler.addFetchRequest(streamChunkId, callback);

    channel.writeAndFlush(new ChunkFetchRequest(streamChunkId)).addListener(future -> {
      if (future.isSuccess()) {
        long timeTaken = System.currentTimeMillis() - startTime;
        if (logger.isTraceEnabled()) {
          logger.trace("Sending request {} to {} took {} ms", streamChunkId,
            getRemoteAddress(channel), timeTaken);
        }
      } else {
        String errorMsg = String.format("Failed to send request %s to %s: %s", streamChunkId,
          getRemoteAddress(channel), future.cause());
        logger.error(errorMsg, future.cause());
        handler.removeFetchRequest(streamChunkId);
        channel.close();
        try {
          callback.onFailure(chunkIndex, new IOException(errorMsg, future.cause()));
        } catch (Exception e) {
          logger.error("Uncaught exception in RPC response callback handler!", e);
        }
      }
    });
  }

  /**
   * Request to stream the data with the given stream ID from the remote end.
   *
   * @param streamId The stream to fetch.
   * @param callback Object to call with the stream data.
   */
  public void stream(String streamId, StreamCallback callback) {
    long startTime = System.currentTimeMillis();
    if (logger.isDebugEnabled()) {
      logger.debug("Sending stream request for {} to {}", streamId, getRemoteAddress(channel));
    }

    // Need to synchronize here so that the callback is added to the queue and the RPC is
    // written to the socket atomically, so that callbacks are called in the right order
    // when responses arrive.
    synchronized (this) {
      handler.addStreamCallback(streamId, callback);
      channel.writeAndFlush(new StreamRequest(streamId)).addListener(future -> {
        if (future.isSuccess()) {
          long timeTaken = System.currentTimeMillis() - startTime;
          if (logger.isTraceEnabled()) {
            logger.trace("Sending request for {} to {} took {} ms", streamId,
              getRemoteAddress(channel), timeTaken);
          }
        } else {
          String errorMsg = String.format("Failed to send request for %s to %s: %s", streamId,
            getRemoteAddress(channel), future.cause());
          logger.error(errorMsg, future.cause());
          channel.close();
          try {
            callback.onFailure(streamId, new IOException(errorMsg, future.cause()));
          } catch (Exception e) {
            logger.error("Uncaught exception in RPC response callback handler!", e);
          }
        }
      });
    }
  }

  /**
   * Sends an opaque message to the RpcHandler on the server-side. The callback will be invoked
   * with the server's response or upon any failure.
   *
   * @param message The message to send.
   * @param callback Callback to handle the RPC's reply.
   * @return The RPC's id.
   */
  public long sendRpc(ByteBuffer message, RpcResponseCallback callback) {
    long startTime = System.currentTimeMillis();
    if (logger.isTraceEnabled()) {
      logger.trace("Sending RPC to {}", getRemoteAddress(channel));
    }

    long requestId = Math.abs(UUID.randomUUID().getLeastSignificantBits());
    handler.addRpcRequest(requestId, callback);

    channel.writeAndFlush(new RpcRequest(requestId, new NioManagedBuffer(message)))
        .addListener(future -> {
          if (future.isSuccess()) {
            long timeTaken = System.currentTimeMillis() - startTime;
            if (logger.isTraceEnabled()) {
              logger.trace("Sending request {} to {} took {} ms", requestId,
                getRemoteAddress(channel), timeTaken);
            }
          } else {
            String errorMsg = String.format("Failed to send RPC %s to %s: %s", requestId,
              getRemoteAddress(channel), future.cause());
            logger.error(errorMsg, future.cause());
            handler.removeRpcRequest(requestId);
            channel.close();
            try {
              callback.onFailure(new IOException(errorMsg, future.cause()));
            } catch (Exception e) {
              logger.error("Uncaught exception in RPC response callback handler!", e);
            }
          }
        });

    return requestId;
  }

  /**
   * Synchronously sends an opaque message to the RpcHandler on the server-side, waiting for up to
   * a specified timeout for a response.
   */
  public ByteBuffer sendRpcSync(ByteBuffer message, long timeoutMs) {
    final SettableFuture<ByteBuffer> result = SettableFuture.create();

    sendRpc(message, new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer response) {
        ByteBuffer copy = ByteBuffer.allocate(response.remaining());
        copy.put(response);
        // flip "copy" to make it readable
        copy.flip();
        result.set(copy);
      }

      @Override
      public void onFailure(Throwable e) {
        result.setException(e);
      }
    });

    try {
      return result.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Sends an opaque message to the RpcHandler on the server-side. No reply is expected for the
   * message, and no delivery guarantees are made.
   *
   * @param message The message to send.
   */
  public void send(ByteBuffer message) {
    channel.writeAndFlush(new OneWayMessage(new NioManagedBuffer(message)));
  }

  /**
   * Removes any state associated with the given RPC.
   *
   * @param requestId The RPC id returned by {@link #sendRpc(ByteBuffer, RpcResponseCallback)}.
   */
  public void removeRpcRequest(long requestId) {
    handler.removeRpcRequest(requestId);
  }

  /** Mark this channel as having timed out. */
  public void timeOut() {
    this.timedOut = true;
  }

  @VisibleForTesting
  public TransportResponseHandler getHandler() {
    return handler;
  }

  @Override
  public void close() {
    // close is a local operation and should finish with milliseconds; timeout just to be safe
    channel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("remoteAdress", channel.remoteAddress())
      .add("clientId", clientId)
      .add("isActive", isActive())
      .toString();
  }
}
