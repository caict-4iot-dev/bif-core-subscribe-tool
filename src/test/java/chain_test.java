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
import cn.bif.api.BIFSDK;
import cn.bif.common.JsonUtils;
import cn.bif.model.request.BIFTransactionGetInfoRequest;
import cn.bif.model.response.BIFTransactionGetInfoResponse;
import cn.caict.bifchain.proto.Chain;
import cn.caict.bifchain.proto.Overlay;
import cn.caict.bifchain.websocket.BlockChainAdapter;
import cn.caict.bifchain.websocket.BlockChainAdapterProc;
import cn.caict.bifchain.utils.ToHex;
/*
  测试须修改的变量：
  1. chain_test类下面的:
        httpUrl是节点IP和端口
        webSocketUrl是节点IP和配置的wsserver的绑定端口
  2. chain_test类下面的TestThread类下的run方法中:
        srcAddress是发送交易的账户地址
 */

public class chain_test {
    private static chain_test chainTest;
    /**
     * BIF-Core-SDK
     */
    String httpUrl = "http://test-bif-core.xinghuo.space";
    BIFSDK sdk = BIFSDK.getInstance(httpUrl);
    /**
     * 订阅服务
     */
    String webSocketUrl = "ws://test-bif-core.xinghuo.space:7301";
    boolean isConnected = false;
    /**
     * 订阅账号
     */
    String srcAddress = "did:bid:efHmvWpqfVzv5rLNSMrhEdNegLz9AcnS";

    /**
     * 消息适配器
     */
    private BlockChainAdapter chain_message_one_;
    public static void main(String[] argv) {

        chainTest = new chain_test();
        chainTest.Initialize();
        System.out.println("*****************start chain_message successfully******************");
        //chainTest.Stop();
    }
    public void Stop() {
        chain_message_one_.Stop();
    }

    /**
     * 订阅初始化
     */
    public void Initialize() {
        chain_message_one_ = new BlockChainAdapter(webSocketUrl);
        //接收hello响应消息
        chain_message_one_.AddChainResponseMethod(Overlay.ChainMessageType.CHAIN_HELLO_VALUE, new BlockChainAdapterProc() {
            public void ChainMethod (byte[] msg, int length) {
                OnChainHello(msg, length);
            }
        });
        //接收区块链实时推送的消息-交易信息
        chain_message_one_.AddChainMethod(Overlay.ChainMessageType.CHAIN_TX_ENV_STORE_VALUE, new BlockChainAdapterProc() {
            public void ChainMethod (byte[] msg, int length) {
                OnChainTxEnvStore(msg, length);
            }
        });
        //接收区块链实时推送的消息-区块信息
        chain_message_one_.AddChainMethod(Overlay.ChainMessageType.CHAIN_LEDGER_TXS_VALUE, new BlockChainAdapterProc() {
            public void ChainMethod (byte[] msg, int length) {
                OnChainLedgerTxs(msg, length);
            }
        });

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //发送hello请求
        Overlay.ChainHello.Builder chain_hello = Overlay.ChainHello.newBuilder();
        chain_hello.setTimestamp(System.currentTimeMillis());
        if (!chain_message_one_.Send(Overlay.ChainMessageType.CHAIN_HELLO.getNumber(), chain_hello.build().toByteArray())) {
            System.out.println("send hello failed");
        }
        //订阅指定账号交易信息
        Overlay.ChainSubscribeTx.Builder tx=Overlay.ChainSubscribeTx.newBuilder();
        tx.addAddress(srcAddress);
        if (!chain_message_one_.Send(Overlay.ChainMessageType.CHAIN_SUBSCRIBE_TX.getNumber(), tx.build().toByteArray())) {
            System.out.println("send tx failed");
        }
    }
    private void OnChainHello(byte[] msg, int length) {
        try {
            Overlay.ChainStatus chain_status = Overlay.ChainStatus.parseFrom(msg);
            isConnected = true;
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    private void OnChainTxEnvStore(byte[] msg, int length) {
        try {
            Chain.TransactionEnvStore  envStore = Chain.TransactionEnvStore.parseFrom(msg);
            System.out.println("OnChainTxEnvStore hash:" + ToHex.bytesToHex(envStore.getHash().toByteArray()));
            if (envStore.getErrorCode() == 0) {
                System.out.println(sdk.getUrl());
                BIFTransactionGetInfoRequest request = new BIFTransactionGetInfoRequest();
                request.setHash(ToHex.bytesToHex(envStore.getHash().toByteArray()));
                BIFTransactionGetInfoResponse response = sdk.getBIFTransactionService().getTransactionInfo(request);
                if (response.getErrorCode() == 0) {
                    System.out.println(JsonUtils.toJSONString(response.getResult()));
                } else {
                    System.out.println(JsonUtils.toJSONString(response));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void OnChainLedgerTxs(byte[] msg, int length) {
        try {
            Overlay.LedgerTxs envStore = Overlay.LedgerTxs.parseFrom(msg);
            System.out.println("header size:" + envStore.getLedgerLength());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
