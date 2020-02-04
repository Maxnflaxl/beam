// Copyright 2020 The Beam Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// test helpers and mocks
#include "test_helpers.h"
WALLET_TEST_INIT
#include "mock_bbs_network.cpp"

// tested module
#include "wallet/client/extensions/broadcast_router.h"

// dependencies
#include "boost/optional.hpp"
using namespace std;
using namespace beam;

namespace
{
    struct MockBroadcastListener : public IBroadcastListener
    {
        using OnMessage = function<void(ByteBuffer&)>;

        MockBroadcastListener(OnMessage func) : m_callback(func) {};

        virtual bool onMessage(uint64_t unused, ByteBuffer&& msg) override
        {
            m_callback(msg);
            return true;
        };

        OnMessage m_callback;
    };

    ByteBuffer CreateMsg(const ByteBuffer& content, BroadcastRouter::ContentType type)
    {
        ByteBuffer msg(MsgHeader::SIZE);
        MsgHeader header(0, // V0
                         0, // V1
                         1, // V2
                         static_cast<uint8_t>(type),
                         static_cast<uint8_t>(content.size()));
        header.write(msg.data());
        std::copy(std::cbegin(content),
                  std::cend(content),
                  std::back_inserter(msg));
        return msg;
    };

    void TestProtocolParsing()
    {
        cout << endl << "Test protocol parser stress" << endl;

        MockBbsNetwork mockNetwork;
        BroadcastRouter broadcastRouter(mockNetwork);
        
        ByteBuffer testContent({'t','e','s','t'});

        uint32_t correctMessagesCount = 0;
        MockBroadcastListener testListener(
            [&correctMessagesCount, &testContent]
            (ByteBuffer& msg)
            {
                ++correctMessagesCount;
                WALLET_CHECK(msg == testContent);
            });

        auto testContentType = BroadcastRouter::ContentType::SwapOffers;
        broadcastRouter.registerListener(testContentType, &testListener);

        WalletID dummyWid;
        dummyWid.m_Channel = proto::Bbs::s_MaxWalletChannels;
        {
            cout << "Case: empty message" << endl;
            ByteBuffer emptyBuf;

            WALLET_CHECK_NO_THROW(
                mockNetwork.SendRawMessage(dummyWid, emptyBuf)
            );
            WALLET_CHECK(correctMessagesCount == 0);
        }
        {
            cout << "Case: message header too short" << endl;
            ByteBuffer data(beam::MsgHeader::SIZE - 2, 't');

            WALLET_CHECK_NO_THROW(
                mockNetwork.SendRawMessage(dummyWid, data)
            );
            WALLET_CHECK(correctMessagesCount == 0);
        }
        {
            cout << "Case: message contain only header" << endl;
            ByteBuffer data(MsgHeader::SIZE, 0);
            MsgHeader header(0,0,1,0,0);
            header.write(data.data());
                        
            WALLET_CHECK_NO_THROW(
                mockNetwork.SendRawMessage(dummyWid, data)
            );
            WALLET_CHECK(correctMessagesCount == 0);
        }
        {
            cout << "Case: unsupported version" << endl;
            ByteBuffer data(MsgHeader::SIZE, 0);
            MsgHeader header(1,2,3,0,0);
            header.write(data.data());
            
            WALLET_CHECK_NO_THROW(
                mockNetwork.SendRawMessage(dummyWid, data)
            );
            WALLET_CHECK(correctMessagesCount == 0);
        }
        {
            cout << "Case: wrong message type" << endl;
            ByteBuffer data(MsgHeader::SIZE, 0);
            MsgHeader header(0,0,1,123,0);
            header.write(data.data());
            
            WALLET_CHECK_NO_THROW(
                mockNetwork.SendRawMessage(dummyWid, data)
            );
            WALLET_CHECK(correctMessagesCount == 0);
        }
        {
            cout << "Case: length more than real" << endl;
            uint32_t bodyLength = 6;
            ByteBuffer data(MsgHeader::SIZE + bodyLength - 1, 0);
            MsgHeader header(0,0,1,0,bodyLength);
            header.write(data.data());
            
            WALLET_CHECK_NO_THROW(
                mockNetwork.SendRawMessage(dummyWid, data)
            );
            WALLET_CHECK(correctMessagesCount == 0);
        }
        {
            cout << "Case: correct message" << endl;

            auto msg = CreateMsg(testContent, testContentType);
            WALLET_CHECK_NO_THROW(
                mockNetwork.SendRawMessage(dummyWid, msg)
            );
            WALLET_CHECK(correctMessagesCount == 1);
        }
        broadcastRouter.unregisterListener(testContentType);

        cout << "Test end" << endl;
    }

    void TestRoutersIntegration()
    {
        cout << endl << "Test routers integration" << endl;

        MockBbsNetwork mockNetwork;
        BroadcastRouter broadcastRouterA(mockNetwork);
        BroadcastRouter broadcastRouterB(mockNetwork);

        // Mock network handles just one subscriber per BBS channel.
        // Also router handles just one subscriber per ContentType.
        // When this constraint will be put off more router could be added to test.

        // BroadcastRouter broadcastRouterC(mockNetwork);

        {
            std::cout << "Case: create, dispatch and check message" << endl;

            uint32_t executed = 0;

            ByteBuffer testSampleA = {'s','w','a','p'};
            ByteBuffer testSampleB = {'u','p','d','a','t','e'};
            // ByteBuffer testSampleC = {'r','a','t','e'};
            MockBroadcastListener testListener(
                [&executed, &testSampleA, testSampleB/*, testSampleC*/]
                (ByteBuffer& msg)
                {
                    ++executed;
                    switch (executed)
                    {
                    case 1:
                        WALLET_CHECK(msg == testSampleA);
                        break;
                    case 2:
                        WALLET_CHECK(msg == testSampleB);
                        break;
                    // case 3:
                    //     WALLET_CHECK(msg == testSampleC);
                    //     break;
                    default:
                        WALLET_CHECK(false);
                        break;
                    }
                });

            broadcastRouterA.registerListener(BroadcastRouter::ContentType::SwapOffers, &testListener);
            broadcastRouterB.registerListener(BroadcastRouter::ContentType::SoftwareUpdates, &testListener);
            // broadcastRouterC.registerListener(BroadcastRouter::ContentType::ExchangeRates, &testListener);

            WalletID dummyWid;
            dummyWid.m_Channel = proto::Bbs::s_MaxWalletChannels;
            auto msgA = CreateMsg(testSampleA, BroadcastRouter::ContentType::SwapOffers);
            mockNetwork.SendRawMessage(dummyWid, msgA);
            dummyWid.m_Channel = proto::Bbs::s_MaxWalletChannels + 1024u;
            auto msgB = CreateMsg(testSampleB, BroadcastRouter::ContentType::SoftwareUpdates);
            mockNetwork.SendRawMessage(dummyWid, msgB);
            // mockNetwork.SendRawMessage(dummyWid, CreateMsg(testSampleC));

            WALLET_CHECK(executed == 2);
        }

        cout << "Test end" << endl;
    }

} // namespace


int main()
{
    cout << "Broadcast router tests:" << endl;

    io::Reactor::Ptr mainReactor{ io::Reactor::create() };
    io::Reactor::Scope scope(*mainReactor);

    TestProtocolParsing();
    TestRoutersIntegration();
    
    assert(g_failureCount == 0);
    return WALLET_CHECK_RESULT;
}
