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

#pragma once

#define BVMOp_Memcpy(macro, sep) \
	macro(void*, pDst) sep \
	macro(const void*, pSrc) sep \
	macro(uint32_t, size)

#define BVMOp_Memcmp(macro, sep) \
	macro(const void*, p1) sep \
	macro(const void*, p2) sep \
	macro(uint32_t, size)

#define BVMOp_Memis0(macro, sep) \
	macro(const void*, p) sep \
	macro(uint32_t, size)

#define BVMOp_Memset(macro, sep) \
	macro(void*, pDst) sep \
	macro(uint8_t, val) sep \
	macro(uint32_t, size)

#define BVMOp_Strlen(macro, sep) \
	macro(const char*, sz)

#define BVMOp_Strcmp(macro, sep) \
	macro(const char*, sz1) sep \
	macro(const char*, sz2)

#define BVMOp_StackAlloc(macro, sep) \
	macro(uint32_t, size)

#define BVMOp_StackFree(macro, sep) \
	macro(uint32_t, size)

#define BVMOp_Heap_Alloc(macro, sep) \
	macro(uint32_t, size)

#define BVMOp_Heap_Free(macro, sep) \
	macro(void*, pPtr)

#define BVMOp_HashCreateSha256(macro, sep)

#define BVMOp_HashCreateKeccak256(macro, sep)

#define BVMOp_HashCreateBlake2b(macro, sep) \
	macro(const void*, pPersonal) sep \
	macro(uint32_t, nPersonal) sep \
	macro(uint32_t, nResultSize)


#define BVMOp_HashWrite(macro, sep) \
	macro(HashObj*, pHash) sep \
	macro(const void*, p) sep \
	macro(uint32_t, size)

#define BVMOp_HashGetValue(macro, sep) \
	macro(HashObj*, pHash) sep \
	macro(void*, pDst) sep \
	macro(uint32_t, size)

#define BVMOp_HashFree(macro, sep) \
	macro(HashObj*, pHash)

#define BVMOp_Secp_Scalar_alloc(macro, sep)

#define BVMOp_Secp_Scalar_free(macro, sep) \
	macro(Secp_scalar&, s)

#define BVMOp_Secp_Scalar_import(macro, sep) \
	macro(Secp_scalar&, s) sep \
	macro(const Secp_scalar_data&, data)

#define BVMOp_Secp_Scalar_export(macro, sep) \
	macro(const Secp_scalar&, s) sep \
	macro(Secp_scalar_data&, data)

#define BVMOp_Secp_Scalar_neg(macro, sep) \
	macro(Secp_scalar&, dst) sep \
	macro(const Secp_scalar&, src)

#define BVMOp_Secp_Scalar_add(macro, sep) \
	macro(Secp_scalar&, dst) sep \
	macro(const Secp_scalar&, a) sep \
	macro(const Secp_scalar&, b)

#define BVMOp_Secp_Scalar_mul(macro, sep) \
	macro(Secp_scalar&, dst) sep \
	macro(const Secp_scalar&, a) sep \
	macro(const Secp_scalar&, b)

#define BVMOp_Secp_Scalar_inv(macro, sep) \
	macro(Secp_scalar&, dst) sep \
	macro(const Secp_scalar&, src)

#define BVMOp_Secp_Scalar_set(macro, sep) \
	macro(Secp_scalar&, dst) sep \
	macro(uint64_t, val)

#define BVMOp_Secp_Point_alloc(macro, sep)

#define BVMOp_Secp_Point_free(macro, sep) \
	macro(Secp_point&, p)

#define BVMOp_Secp_Point_Import(macro, sep) \
	macro(Secp_point&, p) sep \
	macro(const PubKey&, pk)

#define BVMOp_Secp_Point_Export(macro, sep) \
	macro(const Secp_point&, p) sep \
	macro(PubKey&, pk)

#define BVMOp_Secp_Point_neg(macro, sep) \
	macro(Secp_point&, dst) sep \
	macro(const Secp_point&, src)

#define BVMOp_Secp_Point_add(macro, sep) \
	macro(Secp_point&, dst) sep \
	macro(const Secp_point&, a) sep \
	macro(const Secp_point&, b)

#define BVMOp_Secp_Point_mul(macro, sep) \
	macro(Secp_point&, dst) sep \
	macro(const Secp_point&, p) sep \
	macro(const Secp_scalar&, s)

#define BVMOp_Secp_Point_IsZero(macro, sep) \
	macro(const Secp_point&, p)

#define BVMOp_Secp_Point_mul_G(macro, sep) \
	macro(Secp_point&, dst) sep \
	macro(const Secp_scalar&, s)

#define BVMOp_Secp_Point_mul_J(macro, sep) \
	macro(Secp_point&, dst) sep \
	macro(const Secp_scalar&, s)

#define BVMOp_Secp_Point_mul_H(macro, sep) \
	macro(Secp_point&, dst) sep \
	macro(const Secp_scalar&, s) sep \
	macro(AssetID, aid)

#define BVMOp_VerifyBeamHashIII(macro, sep) \
	macro(const void*, pInp) sep \
	macro(uint32_t, nInp) sep \
	macro(const void*, pNonce) sep \
	macro(uint32_t, nNonce) sep \
	macro(const void*, pSol) sep \
	macro(uint32_t, nSol)

#define BVMOp_LoadVar(macro, sep) \
	macro(const void*, pKey) sep \
	macro(uint32_t, nKey) sep \
	macro(void*, pVal) sep \
	macro(uint32_t, nVal) sep \
	macro(uint8_t, nType)

#define BVMOp_SaveVar(macro, sep) \
	macro(const void*, pKey) sep \
	macro(uint32_t, nKey) sep \
	macro(const void*, pVal) sep \
	macro(uint32_t, nVal) sep \
	macro(uint8_t, nType)

#define BVMOp_CallFar(macro, sep) \
	macro(const ContractID&, cid) sep \
	macro(uint32_t, iMethod) sep \
	macro(void*, pArgs) sep \
	macro(uint32_t, nArgs)

#define BVMOp_get_CallDepth(macro, sep)

#define BVMOp_get_CallerCid(macro, sep) \
	macro(uint32_t, iCaller) sep \
	macro(ContractID&, cid)

#define BVMOp_Halt(macro, sep)

#define BVMOp_AddSig(macro, sep) \
	macro(const PubKey&, pubKey)

#define BVMOp_FundsLock(macro, sep) \
	macro(AssetID, aid) sep \
	macro(Amount, amount)

#define BVMOp_FundsUnlock(macro, sep) \
	macro(AssetID, aid) sep \
	macro(Amount, amount)

#define BVMOp_RefAdd(macro, sep) \
	macro(const ContractID&, cid)

#define BVMOp_RefRelease(macro, sep) \
	macro(const ContractID&, cid)

#define BVMOp_AssetCreate(macro, sep) \
	macro(const void*, pMeta) sep \
	macro(uint32_t, nMeta)

#define BVMOp_AssetEmit(macro, sep) \
	macro(AssetID, aid) sep \
	macro(Amount, amount) sep \
	macro(uint8_t, bEmit)

#define BVMOp_AssetDestroy(macro, sep) \
	macro(AssetID, aid)

#define BVMOp_get_Height(macro, sep)

#define BVMOp_get_HdrInfo(macro, sep) \
	macro(BlockHeader::Info&, hdr)

#define BVMOp_get_HdrFull(macro, sep) \
	macro(BlockHeader::Full&, hdr)

#define BVMOp_get_RulesCfg(macro, sep) \
	macro(Height, h) sep \
	macro(HashValue&, res)

#define BVMOp_VarsEnum(macro, sep) \
	macro(const void*, pKey0) sep \
	macro(uint32_t, nKey0) sep \
	macro(const void*, pKey1) sep \
	macro(uint32_t, nKey1)

#define BVMOp_VarsMoveNext(macro, sep) \
	macro(const void**, ppKey) sep \
	macro(uint32_t*, pnKey) sep \
	macro(const void**, ppVal) sep \
	macro(uint32_t*, pnVal)

#define BVMOp_VarGetProof(macro, sep) \
	macro(const void*, pKey) sep \
	macro(uint32_t, nKey) sep \
	macro(const void**, ppVal) sep \
	macro(uint32_t*, pnVal) sep \
	macro(const Merkle::Node**, ppProof)

#define BVMOp_DerivePk(macro, sep) \
	macro(PubKey&, pubKey) sep \
	macro(const void*, pID) sep \
	macro(uint32_t, nID)

#define BVMOp_DocAddGroup(macro, sep) \
	macro(const char*, szID)

#define BVMOp_DocCloseGroup(macro, sep)

#define BVMOp_DocAddNum32(macro, sep) \
	macro(const char*, szID) sep \
	macro(uint32_t, val)

#define BVMOp_DocAddNum64(macro, sep) \
	macro(const char*, szID) sep \
	macro(uint64_t, val)

#define BVMOp_DocAddBlob(macro, sep) \
	macro(const char*, szID) sep \
	macro(const void*, pBlob) sep \
	macro(uint32_t, nBlob)

#define BVMOp_DocAddText(macro, sep) \
	macro(const char*, szID) sep \
	macro(const char*, val)

#define BVMOp_DocAddArray(macro, sep) \
	macro(const char*, szID)

#define BVMOp_DocCloseArray(macro, sep)

#define BVMOp_DocGetText(macro, sep) \
	macro(const char*, szID) sep \
	macro(char*, szRes) sep \
	macro(uint32_t, nLen)

#define BVMOp_DocGetNum32(macro, sep) \
	macro(const char*, szID) sep \
	macro(uint32_t*, pOut)

#define BVMOp_DocGetNum64(macro, sep) \
	macro(const char*, szID) sep \
	macro(uint64_t*, pOut)

#define BVMOp_DocGetBlob(macro, sep) \
	macro(const char*, szID) sep \
	macro(void*, pOut) sep \
	macro(uint32_t, nLen)

#define BVMOp_GenerateKernel(macro, sep) \
	macro(const ContractID*, pCid) sep \
	macro(uint32_t, iMethod) sep \
	macro(const void*, pArg) sep \
	macro(uint32_t, nArg) sep \
	macro(const FundsChange*, pFunds) sep \
	macro(uint32_t, nFunds) sep \
	macro(const SigRequest*, pSig) sep \
	macro(uint32_t, nSig) sep \
	macro(const char*, szComment) sep \
	macro(uint32_t, nCharge)

#define BVMOpsAll_Common(macro) \
	macro(0x10, void*    , Memcpy) \
	macro(0x11, void*    , Memset) \
	macro(0x12, int32_t  , Memcmp) \
	macro(0x13, uint8_t  , Memis0) \
	macro(0x14, uint32_t , Strlen) \
	macro(0x15, int32_t  , Strcmp) \
	macro(0x18, void*    , StackAlloc) \
	macro(0x19, void     , StackFree) \
	macro(0x1A, void*    , Heap_Alloc) \
	macro(0x1B, void     , Heap_Free) \
	macro(0x28, void     , Halt) \
	macro(0x2B, void     , HashWrite) \
	macro(0x2D, void     , HashGetValue) \
	macro(0x2E, void     , HashFree) \
	macro(0x40, Height   , get_Height) \
	macro(0x41, void     , get_HdrInfo) \
	macro(0x42, void     , get_HdrFull) \
	macro(0x43, Height   , get_RulesCfg) \
	macro(0x48, HashObj* , HashCreateSha256) \
	macro(0x49, HashObj* , HashCreateBlake2b) \
	macro(0x4A, HashObj* , HashCreateKeccak256) \
	macro(0x80, Secp_scalar* , Secp_Scalar_alloc) \
	macro(0x81, void     , Secp_Scalar_free) \
	macro(0x82, uint8_t  , Secp_Scalar_import) \
	macro(0x83, void     , Secp_Scalar_export) \
	macro(0x84, void     , Secp_Scalar_neg) \
	macro(0x85, void     , Secp_Scalar_add) \
	macro(0x86, void     , Secp_Scalar_mul) \
	macro(0x87, void     , Secp_Scalar_inv) \
	macro(0x88, void     , Secp_Scalar_set) \
	macro(0x90, Secp_point* , Secp_Point_alloc) \
	macro(0x91, void     , Secp_Point_free) \
	macro(0x92, uint8_t     , Secp_Point_Import) \
	macro(0x93, void     , Secp_Point_Export) \
	macro(0x94, void     , Secp_Point_neg) \
	macro(0x95, void     , Secp_Point_add) \
	macro(0x96, void     , Secp_Point_mul) \
	macro(0x97, uint8_t  , Secp_Point_IsZero) \
	macro(0x98, void     , Secp_Point_mul_G) \
	macro(0x99, void     , Secp_Point_mul_J) \
	macro(0x9A, void     , Secp_Point_mul_H) \
	macro(0xB0, uint8_t  , VerifyBeamHashIII) \

#define BVMOpsAll_Contract(macro) \
	macro(0x20, uint32_t , LoadVar) \
	macro(0x21, uint32_t , SaveVar) \
	macro(0x23, void     , CallFar) \
	macro(0x24, uint32_t , get_CallDepth) \
	macro(0x25, void     , get_CallerCid) \
	macro(0x29, void     , AddSig) \
	macro(0x30, void     , FundsLock) \
	macro(0x31, void     , FundsUnlock) \
	macro(0x32, uint8_t  , RefAdd) \
	macro(0x33, uint8_t  , RefRelease) \
	macro(0x38, AssetID  , AssetCreate) \
	macro(0x39, uint8_t  , AssetEmit) \
	macro(0x3A, uint8_t  , AssetDestroy) \

#define BVMOpsAll_Manager(macro) \
	macro(0x51, void     , VarsEnum) \
	macro(0x52, uint8_t  , VarsMoveNext) \
	macro(0x53, uint32_t , VarGetProof) \
	macro(0x58, void     , DerivePk) \
	macro(0x60, void     , DocAddGroup) \
	macro(0x61, void     , DocCloseGroup) \
	macro(0x62, void     , DocAddText) \
	macro(0x63, void     , DocAddNum32) \
	macro(0x64, void     , DocAddNum64) \
	macro(0x65, void     , DocAddArray) \
	macro(0x66, void     , DocCloseArray) \
	macro(0x67, void     , DocAddBlob) \
	macro(0x69, uint32_t , DocGetText) \
	macro(0x6A, uint8_t  , DocGetNum32) \
	macro(0x6B, uint8_t  , DocGetNum64) \
	macro(0x6C, uint32_t , DocGetBlob) \
	macro(0x70, void     , GenerateKernel)

#define EXTRA_LINE_BEFORE_EOF_SO_THAT_THE_STUPID_COMPILER_WONT_COMPLAIN_ABOUT_BACKSLASH_ON_PREVIOUS_LINE
