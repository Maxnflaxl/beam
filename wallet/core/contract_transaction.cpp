// Copyright 2018 The Beam Team
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

#include "contract_transaction.h"

#include "base_tx_builder.h"
#include "wallet.h"
#include "bvm/ManagerStd.h"
#include "contracts/shaders_manager.h"

namespace beam::wallet
{
    ContractTransaction::Creator::Creator(IWalletDB::Ptr walletDB)
        : m_WalletDB(walletDB)
    {
    }

    BaseTransaction::Ptr ContractTransaction::Creator::Create(const TxContext& context)
    {
        return BaseTransaction::Ptr(new ContractTransaction(context));
    }

    ContractTransaction::ContractTransaction(const TxContext& context)
        : BaseTransaction (TxType::Contract, context)
    {
    }

    bool ContractTransaction::IsInSafety() const
    {
        const auto txState = GetState<State>();
        return txState == State::Registration;
    }

    struct ContractTransaction::MyBuilder
        :public BaseTxBuilder
    {
        using BaseTxBuilder::BaseTxBuilder;

        bvm2::ContractInvokeData m_Data;
        const HeightHash* m_pParentCtx = nullptr;
        uint32_t m_TxMask = 0;

        static void Fail(const char* sz = nullptr)
        {
            throw TransactionFailedException(false, TxFailureReason::Unknown, sz);
        }

        struct SigState
        {
            TxKernelContractControl* m_pKrn;
            uint32_t m_RcvMask = 0;
            bool m_Sent = false;
        };

        std::vector<SigState> m_vSigs;

        struct Channel
            :public IRawCommGateway::IHandler
        {
            MyBuilder* m_pThis;
            WalletID m_WidMy;
            WalletID m_WidPeer;

            Channel()
            {
            }

            virtual ~Channel()
            {
                m_pThis->m_Tx.GetGateway().Unlisten(m_WidMy, this);
            }

            uint32_t get_Idx() const
            {
                return static_cast<uint32_t>(this - &m_pThis->m_vChannels.front());
            }

            static void DeriveSharedSk(ECC::Scalar::Native& skOut, const ECC::Scalar::Native& skMy, const ECC::Point::Native& ptForeign)
            {
                ECC::Point::Native pt = ptForeign * skMy;
                ECC::Point pk = pt;

                ECC::Oracle o;
                o << "dh.contract";
                o << pk;
                o >> skOut;
            }

            void OnMsg(const Blob& d) override
            {
                try
                {
                    Deserializer der;
                    der.reset(d.p, d.n);
                    m_pThis->OnMsg(der, get_Idx());

                    m_pThis->m_Tx.UpdateAsync();
                }
                catch (std::exception&)
                {
                    // ignore
                }
            }

            void Send(Serializer& ser)
            {
                auto res = ser.buffer();
                m_pThis->m_Tx.GetGateway().Send(m_WidPeer, Blob(res.first, (uint32_t)res.second));
            }

            void SendSig(const ECC::Scalar& k, uint32_t iSig)
            {
                Serializer ser;
                ser
                    & iSig
                    & k;

                Send(ser);
            }
        };

        std::vector<Channel> m_vChannels;

        void OnMsg(Deserializer& der, uint32_t iCh)
        {
            uint32_t msk = 1u << iCh;
            uint32_t iSig = 0;
            der & iSig;

            if (iSig < m_vSigs.size())
            {
                auto& st = m_vSigs[iSig];
                if (st.m_RcvMask & msk)
                    Fail();

                ECC::Scalar val;
                der & val;

                if (m_Data.m_IsSender)
                    // partial sig
                    AddScalar(st.m_pKrn->m_Signature.m_k, val);
                else
                    st.m_pKrn->m_Signature.m_k = val; // final sig

                st.m_RcvMask |= msk;
            }
            else
            {
                if (!m_Data.m_IsSender)
                    Fail();

                // tx part
                if (m_TxMask & msk)
                    Fail();
                m_TxMask |= msk;

                Transaction tx;
                der & tx;

                MoveVectorInto(m_pTransaction->m_vInputs, tx.m_vInputs);
                MoveVectorInto(m_pTransaction->m_vOutputs, tx.m_vOutputs);
                MoveVectorInto(m_pTransaction->m_vKernels, tx.m_vKernels);
                AddScalar(m_pTransaction->m_Offset, tx.m_Offset);
            }
        }


        void AddCoinOffsets(const Key::IKdf::Ptr& pKdf)
        {
            ECC::Scalar::Native kOffs;
            m_Coins.AddOffset(kOffs, pKdf);
            AddOffset(kOffs);
        }

        void OnSigned()
        {
            m_pKrn = m_pTransaction->m_vKernels.front().get(); // if there're many - let it be the 1st contract kernel
            SaveKernel();
            SaveKernelID();
            SaveInOuts();
        }

        void TestSigs()
        {
            for (uint32_t i = 0; i < m_Data.m_vec.size(); i++)
            {
                const auto& cdata = m_Data.m_vec[i];
                if (!cdata.IsMultisigned())
                    continue;

                auto& krn = Cast::Up<TxKernelContractControl>(*m_pTransaction->m_vKernels[i]);

                ECC::Point::Native pt1 = ECC::Context::get().G * ECC::Scalar::Native(krn.m_Signature.m_k);
                ECC::Point::Native pt2;
                pt2.Import(cdata.m_Adv.m_SigImage);

                pt2 += pt1;
                if (pt2 != Zero)
                    Fail("incorrect multisig");

                if (!m_vSigs.empty())
                    krn.UpdateID(); // signature was modified due to the negotiation
            }
        }

        static void AddScalar(ECC::Scalar& dst, const ECC::Scalar& src)
        {
            ECC::Scalar::Native k(dst);
            k += ECC::Scalar::Native(src);
            dst = k;
        }

        template <typename T>
        static void MoveVectorInto(std::vector<T>& dst, std::vector<T>& src)
        {
            size_t n = dst.size();
            dst.resize(n + src.size());

            for (size_t i = 0; i < src.size(); i++)
                dst[n + i] = std::move(src[i]);

            src.clear();
        }

        struct AppShaderExec :public wallet::ManagerStdInWallet
        {
            typedef std::unique_ptr<AppShaderExec> Ptr;

            MyBuilder* m_pBuilder; // set to null when finished
            bool m_Err = false;

            using ManagerStdInWallet::ManagerStdInWallet;

            void SwapParams()
            {
                m_BodyManager.swap(m_pBuilder->m_Data.m_AppInvoke.m_App);
                m_BodyContract.swap(m_pBuilder->m_Data.m_AppInvoke.m_Contract);
                m_Args.swap(m_pBuilder->m_Data.m_AppInvoke.m_Args);
            }

            void OnDone(const std::exception* pExc) override
            {
                assert(m_pBuilder);

                m_Err = !!pExc;
                if (pExc)
                    std::cout << "Shader exec error: " << pExc->what() << std::endl;
                else
                    std::cout << "Shader output: " << m_Out.str() << std::endl;

                m_pBuilder->m_Tx.UpdateAsync();
                m_pBuilder = nullptr;
            }
        };

        AppShaderExec::Ptr m_pAppExec;

        void SetParentCtx()
        {
            m_pParentCtx = nullptr;

            for (const auto& cdata : m_Data.m_vec)
            {
                if (bvm2::ContractInvokeEntry::Flags::Dependent & cdata.m_Flags)
                {
                    m_pParentCtx = &cdata.m_ParentCtx;
                    break;
                }
            }
        }
    };

    void ContractTransaction::Init()
    {
        assert(!m_TxBuilder);

        m_TxBuilder = std::make_shared<MyBuilder>(*this, kDefaultSubTxID);
        auto& builder = *m_TxBuilder;

        GetParameter(TxParameterID::ContractDataPacked, builder.m_Data, GetSubTxID());

        builder.SetParentCtx();
    }

    bool ContractTransaction::BuildTxOnce()
    {
        Key::IKdf::Ptr pKdf = get_MasterKdfStrict();

        auto& builder = *m_TxBuilder;
        auto& vData = builder.m_Data;

        auto s = GetState<State>();

        if (State::RebuildHft == s)
        {
            if (!builder.m_pAppExec)
            {
                builder.m_pAppExec = std::make_unique<MyBuilder::AppShaderExec>(m_Context.get_Wallet());
                auto& aex = *builder.m_pAppExec;
                aex.m_pBuilder = &builder;
                aex.SwapParams();
                aex.set_Privilege(vData.m_AppInvoke.m_Privilege);

                builder.m_pAppExec->StartRun(1);
            }

            auto& aex = *builder.m_pAppExec;
            if (aex.m_pBuilder)
                return false; // still running

            if (aex.m_Err)
            {
                OnFailed(TxFailureReason::TransactionExpired);
                return false;
            }

            if (aex.m_InvokeData.m_vec.empty())
            {
                OnFailed(TxFailureReason::TransactionExpired);
                return false;
            }

            // TODO: check slipage is within limits

            Cast::Down<bvm2::ContractInvokeDataBase>(vData) = std::move(aex.m_InvokeData);

            aex.m_pBuilder = &builder;
            aex.SwapParams();

            builder.m_pAppExec.reset();

            builder.SetParentCtx();

            s = State::Initial;
            SetState(s);
        }

        if (State::Initial == s)
        {
            UpdateTxDescription(TxStatus::InProgress);

            if (builder.m_pParentCtx)
                builder.m_Height = builder.m_pParentCtx->m_Height;
            else
            {
                Block::SystemState::Full sTip;
                if (!GetTip(sTip))
                    return false;

                builder.m_Height.m_Min = sTip.m_Height + 1;
                builder.m_Height.m_Max = sTip.m_Height + 5; // 5 blocks - standard contract tx life time
            }

            if (vData.m_vec.empty())
                builder.Fail();

            bvm2::FundsMap fm = vData.get_FullSpend();

            for (uint32_t i = 0; i < vData.m_vec.size(); i++)
            {
                const auto& cdata = vData.m_vec[i];

                Amount fee;
                if (cdata.IsAdvanced())
                    fee = cdata.m_Adv.m_Fee; // can't change!
                else
                {
                    fee = cdata.get_FeeMin(builder.m_Height.m_Min);
                    if (!i)
                        std::setmax(fee, builder.m_Fee);
                }

                cdata.Generate(*builder.m_pTransaction, *pKdf, builder.m_Height, fee);

                if (vData.m_IsSender)
                    fm[0] += fee;
            }

            BaseTxBuilder::Balance bb(builder);
            for (auto it = fm.begin(); fm.end() != it; it++)
                bb.m_Map[it->first].m_Value -= it->second;

            bb.CompleteBalance(); // will select coins as needed
            builder.SaveCoins();

            builder.AddCoinOffsets(pKdf);
            builder.OnSigned();

            s = State::GeneratingCoins;
            SetState(s);
        }

        if (builder.m_vSigs.empty() && vData.HasMultiSig())
        {
            if (vData.m_vPeers.empty())
                builder.Fail("no peers");

            builder.m_vChannels.reserve(vData.m_vPeers.size());

            ECC::Scalar::Native skMy;
            pKdf->DeriveKey(skMy, vData.m_hvKey);

            for (const auto& pk : vData.m_vPeers)
            {
                auto& c = builder.m_vChannels.emplace_back();
                c.m_pThis = &builder;

                ECC::Point::Native pt;
                if (!pt.ImportNnz(pk))
                    builder.Fail("bad peer");

                ECC::Scalar::Native sk, skMul;
                c.DeriveSharedSk(skMul, skMy, pt);

                sk = skMy * skMul;
                pt = pt * skMul;

                c.m_WidMy.m_Pk.FromSk(sk);
                c.m_WidMy.SetChannelFromPk();

                c.m_WidPeer.m_Pk.Import(pt);
                c.m_WidPeer.SetChannelFromPk();

                GetGateway().Listen(c.m_WidMy, sk, &c);
            }

            for (uint32_t i = 0; i < vData.m_vec.size(); i++)
            {
                const auto& cdata = vData.m_vec[i];
                if (!cdata.IsMultisigned())
                    continue;

                auto& st = builder.m_vSigs.emplace_back();
                st.m_pKrn = &Cast::Up<TxKernelContractControl>(*builder.m_pTransaction->m_vKernels[i]);
            }
        }

        if (State::GeneratingCoins == s)
        {
            builder.GenerateInOuts();
            if (builder.IsGeneratingInOuts())
                return false;

            if (builder.m_vSigs.empty())
            {
                builder.TestSigs();
                builder.FinalyzeTx();
                s = State::Registration;
            }
            else
                s = State::Negotiating;

            SetState(s);
        }

        if (State::Negotiating == s)
        {
            bool bStillNegotiating = false;

            uint32_t msk = (1u << builder.m_vChannels.size()) - 1;

            for (uint32_t iSig = 0; iSig < builder.m_vSigs.size(); iSig++)
            {
                auto& st = builder.m_vSigs[iSig];
                bool bSomeMissing = (st.m_RcvMask != msk);
                if (bSomeMissing)
                    bStillNegotiating = true;

                if (!st.m_Sent)
                {
                    if (vData.m_IsSender && bSomeMissing)
                        continue; // not ready yet

                    for (auto& ch : builder.m_vChannels)
                        ch.SendSig(st.m_pKrn->m_Signature.m_k, iSig);

                    st.m_Sent = true;
                }
            }

            if (!bStillNegotiating && (builder.m_TxMask != msk))
            {
                if (vData.m_IsSender)
                    bStillNegotiating = true;
                else
                {
                    builder.TestSigs();

                    Serializer ser;

                    // time to send my tx part. Exclude the multisig kernels
                    {
                        std::vector<TxKernel::Ptr> v1, v2;

                        for (uint32_t i = 0; i < vData.m_vec.size(); i++)
                        {
                            bool bMultisig = vData.m_vec[i].IsMultisigned();
                            (bMultisig ? v1 : v2).push_back(std::move(builder.m_pTransaction->m_vKernels[i]));
                        }
                        builder.m_pTransaction->m_vKernels = std::move(v2);

                        ser
                            & builder.m_vSigs.size()
                            & *builder.m_pTransaction;

                        builder.m_pTransaction->m_vKernels.swap(v2);

                        uint32_t n1 = 0, n2 = 0;
                        for (uint32_t i = 0; i < vData.m_vec.size(); i++)
                        {
                            bool bMultisig = vData.m_vec[i].IsMultisigned();
                            auto& pKrn = bMultisig ? v1[n1++] : v2[n2++];
                            builder.m_pTransaction->m_vKernels.push_back(std::move(pKrn));
                        }
                    }

                    for (auto& ch : builder.m_vChannels)
                        ch.Send(ser);

                    builder.m_TxMask = msk;
                }
            }

            if (bStillNegotiating)
                return false;

            builder.OnSigned();

            if (vData.m_IsSender)
            {
                builder.TestSigs();
                builder.FinalyzeTx();
            }

            s = Registration;
            SetState(s);
        }

        return (State::Registration == s);
    }

    void ContractTransaction::UpdateImpl()
    {
        if (!m_TxBuilder)
            Init();

        if (!BuildTxOnce())
            return;

        int ret = RegisterTx();
        if (ret < 0)
        {
            // expired. Check if it's an HFT tx that can be rebuilt
            if (RetryHft())
            {
                LOG_INFO() << "TxoID=" << GetTxID() << " Expired. Retrying HFT tx";
                UpdateAsync();
            }
            else
                OnFailed(TxFailureReason::TransactionExpired);
        }
    }

    int ContractTransaction::RegisterTx()
    {
        Height h = 0;
        GetParameter(TxParameterID::KernelProofHeight, h);
        if (h)
        {
            SetCompletedTxCoinStatuses(h);
            CompleteTx();
            return 1;
        }

        auto& builder = *m_TxBuilder;

        GetParameter(TxParameterID::KernelUnconfirmedHeight, h);

        if (h && IsExpired(h + 1))
            return -1;

        if (builder.m_Data.m_IsSender)
        {
            // We're the tx owner
            uint8_t nTxRegStatus = proto::TxStatus::Unspecified;
            if (!GetParameter(TxParameterID::TransactionRegistered, nTxRegStatus))
            {
                Block::SystemState::Full sTip;
                if (GetTip(sTip) && !IsExpired(sTip.m_Height + 1))
                    GetGateway().register_tx(GetTxID(), builder.m_pTransaction, builder.m_pParentCtx ? &builder.m_pParentCtx->m_Hash : nullptr);
            }
            else
            {
                // assume the status could be due to redundant tx send. Ensure tx hasn't been accepted yet
                if ((proto::TxStatus::Ok != nTxRegStatus) && h)
                {
                    switch (nTxRegStatus)
                    {
                    case proto::TxStatus::DependentNoParent:
                    case proto::TxStatus::DependentNotBest:
                    case proto::TxStatus::DependentNoNewCtx:
                        return -1;

                    default:
                        OnFailed(TxFailureReason::FailedToRegister, true);
                        return 0;
                    }
                }
            }
        }

        ConfirmKernel(builder.m_pKrn->m_Internal.m_ID);

        return 0;
    }

    bool ContractTransaction::IsExpired(Height hTrg)
    {
        auto& builder = *m_TxBuilder;

        Height hMax;
        if (builder.m_pParentCtx)
            hMax = builder.m_pParentCtx->m_Height;
        else
        {
            if (builder.m_pKrn)
                hMax = builder.m_pKrn->get_EffectiveHeightRange().m_Max;
            else
            {
                if (!GetParameter(TxParameterID::MaxHeight, hMax))
                    return false;
            }
        }

        return (hMax < hTrg);
    }

    bool ContractTransaction::CheckExpired()
    {
        return false; // disable the outer logic, handle expiration internally
    }

    bool ContractTransaction::CanCancel() const
    {
        if (!m_TxBuilder)
            return true;

        if (m_TxBuilder->m_Data.m_IsSender)
            return true;

        return GetState<State>() != State::Registration;
    }

    bool ContractTransaction::RetryHft()
    {
        auto& builder = *m_TxBuilder;

        if (!builder.m_pParentCtx || builder.m_Data.m_AppInvoke.m_App.empty())
            return false;

        Height h = 0;
        GetParameter(TxParameterID::MinHeight, h);
        if (!h)
        {
            h = m_TxBuilder->m_pKrn->m_Height.m_Min;
            SetParameter(TxParameterID::MinHeight, h);
        }

        Block::SystemState::Full sTip;
        if (!GetTip(sTip))
            return false; //?!

        if (sTip.m_Height - h >= 5)
            return false;

        // Release coins, Reset everything
        SetParameter(TxParameterID::KernelUnconfirmedHeight, Zero);
        SetParameter(TxParameterID::TransactionRegistered, Zero);

        {
            auto pDB = GetWalletDB();
            for (const auto& cid : builder.m_Coins.m_Input)
            {
                Coin coin;
                coin.m_ID = cid;
                if (pDB->findCoin(coin))
                {
                    coin.m_spentTxId.reset();
                    pDB->saveCoin(coin);
                }
            }

            for (auto& cid : builder.m_Coins.m_InputShielded)
            {
                auto pCoin = pDB->getShieldedCoin(cid.m_Key);
                if (pCoin)
                {
                    pCoin->m_spentTxId.reset();
                    pDB->saveShieldedCoin(*pCoin);
                }
            }

            pDB->deleteCoinsCreatedByTx(GetTxID());
        }

        SetParameter(TxParameterID::InputCoins, Zero);
        SetParameter(TxParameterID::InputCoinsShielded, Zero);
        SetParameter(TxParameterID::OutputCoins, Zero);

        SetParameter(TxParameterID::Inputs, Zero);
        SetParameter(TxParameterID::ExtraKernels, Zero); // shielded inputs probably
        SetParameter(TxParameterID::Outputs, Zero);

        SetParameter(TxParameterID::Offset, Zero);

        SetParameter(TxParameterID::Kernel, Zero);
        SetParameter(TxParameterID::KernelID, Zero);

        bvm2::ContractInvokeData vData = std::move(builder.m_Data);
        m_TxBuilder = std::make_shared<MyBuilder>(*this, kDefaultSubTxID); // reset it
        m_TxBuilder->m_Data = std::move(vData);

        SetState(State::RebuildHft);
        return true;
    }

}
