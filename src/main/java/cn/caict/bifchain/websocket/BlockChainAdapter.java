/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * © COPYRIGHT 2021 Corporation CAICT All rights reserved.
 * http://www.caict.ac.cn
 */
package cn.caict.bifchain.websocket;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import cn.caict.bifchain.proto.Common.*;
import cn.caict.bifchain.proto.Overlay;
import org.java_websocket.WebSocket;
import org.java_websocket.WebSocketImpl;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft;
import org.java_websocket.drafts.Draft_17;
import org.java_websocket.handshake.ServerHandshake;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;


public class BlockChainAdapter {
    private Map<Integer, BlockChainManager> blockchain_managers_ = new ConcurrentHashMap<Integer, BlockChainManager>();
    private BlockChainAdapterProc request_handler_[] = new BlockChainAdapterProc[256];
    private BlockChainAdapterProc response_handler_[] = new BlockChainAdapterProc[256];
    private final long check_interval = 5000;
    private final int OVERLAY_PING = 1;

    private class BlockChainManager implements Runnable {
        private SendMessageThread send_message_thread_;
        private HeartbeatThread heartbeat_thread_;
        private Thread blockchain_manager_thhead;
        private BlockChain block_chain_;
        private LinkedBlockingQueue<WsMessage> send_queue_;
        private boolean is_exit = true;
        private long sequence_ = 0;
        private long heartbeat_time_ = 0;
        private Integer index_ = -1;
        private Draft draft_;
        private URI uri_;
        public BlockChainManager(Integer index, String uri_address) {
            index_ = index;
            draft_ = new Draft_17();
            uri_ = URI.create(uri_address);
            send_queue_ = new LinkedBlockingQueue<WsMessage>();
            blockchain_manager_thhead = new Thread(this);
            blockchain_manager_thhead.start();
            send_message_thread_ = new SendMessageThread("Send Thread");
            heartbeat_thread_ = new HeartbeatThread("Heartbeat");
        }

        // start thread
        @Override
        public void run() {
            if (!is_exit) {
                System.out.println("Already running");
                return;
            }
            is_exit = false;
            while (!is_exit) {
                try {
                    block_chain_ = new BlockChain(draft_, uri_);
                    Thread block_chain_thread = new Thread(block_chain_);
                    block_chain_thread.setName("worker_thread_" + index_);
                    block_chain_thread.start();
                    block_chain_thread.join();
                } catch (Exception e) {
                    System.out.println("Connect failed, " + e.getMessage());
                }

                try {
                    Thread.sleep(5000);
                } catch (Exception e) {
                    System.out.println("Sleep 5000 failed, " + e.getMessage());
                }
            }
        }

        public boolean IsConnected() {
            return block_chain_ != null && block_chain_.getConnection().isOpen();
        }

        public void Stop() {
            try {
                is_exit = true;
                if (block_chain_ != null && !block_chain_.getConnection().isClosed()) {
                    block_chain_.getConnection().closeConnection(0, "Stop");
                }
                if (heartbeat_thread_ != null) {
                    heartbeat_thread_.Join();
                    heartbeat_thread_.Stop();
                }
                if (send_message_thread_ != null) {
                    send_message_thread_.Stop();
                    send_message_thread_.Join();
                }

            } catch (Exception e) {
                System.out.println("Join failed, " + e.getMessage());
            }
        }
        public void Join() {
            try {
                blockchain_manager_thhead.join();
            } catch (InterruptedException e) {
                System.out.println("BlockChainManager join error, " + e.getMessage());
            }
        }

        private class BlockChain extends WebSocketClient {
            public BlockChain(Draft d, URI uri) {
                super(uri, d);
                WebSocketImpl.DEBUG = false;

                AddChainMethod(OVERLAY_PING, new BlockChainAdapterProc() {
                    @Override
                    public void ChainMethod (byte[] msg, int length) {
                        OnRequestPing(msg, length);
                    }
                });
                AddChainResponseMethod(OVERLAY_PING, new BlockChainAdapterProc() {
                    @Override
                    public void ChainMethod (byte[] msg, int length) {
                        OnResponsePing(msg, length);
                    }
                });
            }

            @Override
            public void onError( Exception ex ) {
                System.out.println( "Error: " + ex.getMessage());
            }

            @Override
            public void onOpen( ServerHandshake handshake ) {
                heartbeat_time_ = System.currentTimeMillis();
                System.out.println("Open successful");
            }

            @Override
            public void onClose( int code, String reason, boolean remote ) {
                System.out.println( "Closed: " + index_ + ", code:" + code + ", reason:" + reason);
            }

            @Override
            public void onMessage(String message) {
                try {
                    WsMessage wsMessage = WsMessage.parseFrom(ByteString.copyFrom(message.getBytes()));
                    int type = (int)wsMessage.getType();
                    boolean request = wsMessage.getRequest();
                    if (request && request_handler_[type] != null) {
                        byte[] msg = wsMessage.getData().toByteArray();
                        request_handler_[type].ChainMethod(msg, msg.length);
                    }
                    else if (!request && response_handler_[type] != null) {
                        byte[] msg = wsMessage.getData().toByteArray();
                        response_handler_[type].ChainMethod(msg, msg.length);
                    }
                    else {
                        System.out.println( "onMessage:" + (request ? " request method" : " reponse method") + " (" + type + ")" + " does not exist");
                    }
                } catch (Exception e) {
                    System.out.println("The message of receive is not WsMessage format");
                }
            }

            @Override
            public void onMessage(ByteBuffer message) {
                try {
                    WsMessage wsMessage = WsMessage.parseFrom(ByteString.copyFrom(message));
                    int type = (int)wsMessage.getType();
                    boolean request = wsMessage.getRequest();
                    if (request && request_handler_[type] != null) {
                        byte[] msg = wsMessage.getData().toByteArray();
                        request_handler_[type].ChainMethod(msg, msg.length);
                    }
                    else if (!request && response_handler_[type] != null) {
                        byte[] msg = wsMessage.getData().toByteArray();
                        response_handler_[type].ChainMethod(msg, msg.length);
                    }
                    else {
                        System.out.println( "onMessage:" + (request ? " request method" : " reponse method") + " (" + type + ")" + " does not exist");
                    }
                } catch (Exception e) {
                    System.out.println("The message of receive is not WsMessage format");
                }
            }

            private void OnRequestPing(byte[] msg, int length) {
                WebSocket conn = getConnection();
                try {
                    Ping ping = Ping.parseFrom(msg);

                    Pong.Builder pong = Pong.newBuilder();
                    pong.setNonce(ping.getNonce());

                    heartbeat_time_ = System.currentTimeMillis();

                    SendMessage(blockchain_managers_.get(index_), Overlay.OVERLAY_MESSAGE_TYPE.OVERLAY_MSGTYPE_PING_VALUE, false,
                            sequence_, pong.build().toByteArray());
                    System.out.println("Response pong to " + conn.getRemoteSocketAddress().getHostString()
                            + ":" + conn.getRemoteSocketAddress().getPort());
                } catch (InvalidProtocolBufferException e) {
                    System.out.println("OnRequestPing: parse ping data failed" + " (" + conn.getRemoteSocketAddress().getHostString()
                            + ":" + conn.getRemoteSocketAddress().getPort() + ")");
                }
            }

            private void OnResponsePing(byte[] msg, int length) {
                WebSocket conn = getConnection();
                try {
                    Pong.parseFrom(msg);
                    heartbeat_time_ = System.currentTimeMillis();
                    System.out.println("OnRequestPing: Recv pong from " + conn.getRemoteSocketAddress().getHostString()
                            + ":" + conn.getRemoteSocketAddress().getPort());
                } catch (InvalidProtocolBufferException e) {
                    System.out.println("OnResponsePing: parse pong data failed" + " (" + conn.getRemoteSocketAddress().getHostString()
                            + ":" + conn.getRemoteSocketAddress().getPort() + ")");
                }
            }
        }

        private class HeartbeatThread implements Runnable {
            private boolean heartbeat_enabled_ = true;
            private Thread heartbeat_message_thread_;

            HeartbeatThread(String thread_name) {
                heartbeat_enabled_ = true;
                heartbeat_message_thread_ = new Thread(this);
                heartbeat_message_thread_.setName(thread_name);
                heartbeat_message_thread_.start();
            }
            @Override
            public void run() {
                while (heartbeat_enabled_) {
                    try {
                        Thread.sleep(check_interval);
                    } catch (Exception ex) {
                        System.out.println("HeartbeatThread sleep failed, " + ex.getMessage());
                    }
                    if (block_chain_ != null && block_chain_.getConnection().isOpen()) {
                        // send ping
                        WebSocket conn = block_chain_.getConnection();

                        Ping.Builder ping = Ping.newBuilder();
                        ping.setNonce(System.currentTimeMillis() * 1000);
                        SendMessage(blockchain_managers_.get(index_), Overlay.OVERLAY_MESSAGE_TYPE.OVERLAY_MSGTYPE_PING_VALUE,
                                true, sequence_++, ping.build().toByteArray());
                        System.out.println("OnRequestPing: Send ping to " + conn.getRemoteSocketAddress().getHostString()
                                + ":" + conn.getRemoteSocketAddress().getPort());
                    }
                }
            }
            void Stop() {
                heartbeat_enabled_ = false;
            }
            void Join() {
                try {
                    heartbeat_message_thread_.join();
                } catch (InterruptedException e) {
                    System.out.println("HeartbeatThread join error, " + e.getMessage());
                }
            }
        }

        private class SendMessageThread implements Runnable {
            private boolean send_enabled_ = true;
            private Thread send_message_thread_;

            SendMessageThread(String thread_name) {
                send_enabled_ = true;
                send_message_thread_ = new Thread(this);
                send_message_thread_.setName(thread_name);
                send_message_thread_.start();
            }
            @Override
            public void run() {
                while (send_enabled_) {
                    WsMessage send_message = null;
                    try {
                        if (block_chain_ == null || !block_chain_.getConnection().isOpen()) {
                            Thread.sleep(3000);
                            continue;
                        }

                        send_message = send_queue_.take();
                        if (send_message == null) {
                            continue;
                        }

                        block_chain_.send(send_message.toByteArray());
                    }
                    catch(Exception e) {
                        System.out.println("HeartbeatThread send failed, " + e.getMessage());
                        send_queue_.add(send_message);
                        try {
                            Thread.sleep(3000);
                        } catch (Exception ex) {
                            System.out.println("HeartbeatThread sleep failed, " + ex.getMessage());
                        }
                    }
                }
            }

            void Stop() {
                send_enabled_ = false;
                try {
                    WsMessage.Builder send_message = WsMessage.newBuilder();
                    send_message.setData(ByteString.copyFrom("".getBytes()));
                    send_message.setSequence(0);
                    send_message.setRequest(true);
                    send_message.setType(-1);
                    send_queue_.put(send_message.build());
                } catch (InterruptedException e) {
                    System.out.println("MessageThread Stop Error, " + e.getMessage());
                }
            }

            void Join() {
                try {
                    send_message_thread_.join();
                } catch (InterruptedException e) {
                    System.out.println("SendMessageThread join error, " + e.getMessage());
                }
            }

        }
    }

    public BlockChainAdapter(String uri_address) {
        String[] uri_addresses = uri_address.split(";");
        for (Integer i = 0; i < uri_addresses.length; i++) {
            BlockChainManager blockchain_manager = new BlockChainManager(i, uri_addresses[i]);
            blockchain_managers_.put(i, blockchain_manager);
        }
    }

    // stop thread
    public void Stop() {
        for(int i = 0; i < blockchain_managers_.size(); i++) {
            blockchain_managers_.get(i).Stop();
            blockchain_managers_.get(i).Join();
        }
    }

    // start status
    public boolean IsConnected() {
        boolean is_connect = false;
        for(int i = 0; i < blockchain_managers_.size(); i++) {
            if (blockchain_managers_.get(i).IsConnected()) {
                is_connect = true;
                break;
            }
        }
        return is_connect;
    }

    // add callback function
    // add response method to BlockChainAdapter
    public void AddChainMethod(int type, BlockChainAdapterProc chainMethodProc) {
        request_handler_[type] = chainMethodProc;
    }
    // add request method to BlockChainAdapter
    public void AddChainResponseMethod(int type, BlockChainAdapterProc chainRequestMethodProc) {
        response_handler_[type] = chainRequestMethodProc;
    }

    // send message
    public boolean Send(int type, byte[] msg) {
        boolean bret = false;
        do {
            try {
                if (!IsConnected()) {
                    System.out.println("Disconnected");
                    bret = false;
                    break;
                }

                for(int i = 0; i < blockchain_managers_.size(); i++) {
                    BlockChainManager blockchain_manager = blockchain_managers_.get(i);
                    if (blockchain_manager.IsConnected()) {
                        if (!SendMessage(blockchain_manager, type, true, blockchain_manager.sequence_, msg)) {
                            System.out.println("Add send queue failed");
                        }
                    }
                }

                bret =  true;
            } catch(Exception e) {
                System.out.println("Add message failed, " + e.getMessage());
            }
        } while (false);


        return bret;
    }

    private boolean SendMessage(BlockChainManager blockchain_manager, int type, boolean request, long sequence, byte[] data) {
        WsMessage.Builder wsMessage = WsMessage.newBuilder();
        wsMessage.setType(type);
        wsMessage.setRequest(request);
        wsMessage.setSequence(sequence);
        if (data != null) {
            wsMessage.setData(ByteString.copyFrom(data));
        }
        return blockchain_manager.send_queue_.add(wsMessage.build());
    }
}
