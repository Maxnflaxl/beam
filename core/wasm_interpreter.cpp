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

#define _CRT_SECURE_NO_WARNINGS // sprintf
#include "wasm_interpreter.h"
#include <sstream>

#define MY_TOKENIZE2(a, b) a##b
#define MY_TOKENIZE1(a, b) MY_TOKENIZE2(a, b)

namespace beam {
namespace Wasm {

	void Fail() {
		throw std::runtime_error("wasm");
	}
	void Test(bool b) {
		if (!b)
			Fail();
	}

	/////////////////////////////////////////////
	// Reader

	void Reader::Ensure(uint32_t n)
	{
		Test(static_cast<size_t>(m_p1 - m_p0) >= n);
	}

	const uint8_t* Reader::Consume(uint32_t n)
	{
		Ensure(n);

		const uint8_t* pRet = m_p0;
		m_p0 += n;

		return pRet;
	}

	template <bool bSigned>
	uint64_t Reader::ReadInternal()
	{
		uint64_t ret = 0;
		for (uint32_t nShift = 0; ; )
		{
			uint8_t n = Read1();
			bool bEnd = !(0x80 & n);
			if (!bEnd)
				n &= ~0x80;

			ret |= uint64_t(n) << nShift;

			nShift += 7;
			if (nShift >= sizeof(ret) * 8)
				break;

			if (bEnd)
			{
				if constexpr (bSigned)
				{
					if (0x40 & n)
						ret |= (~static_cast<uint64_t>(0) << nShift);
				}
				break;
			}
		}

		return ret;
	}

	template <typename T>
	T Reader::Read()
	{
		uint64_t x = ReadInternal<std::numeric_limits<T>::is_signed>();

		static_assert(sizeof(T) <= sizeof(uint64_t));
		T ret = static_cast<T>(x);
		// Test(ret == static_cast<uint64_t>(x)); // overflow test. Skip currently (not really important)

		return ret;
	}

	/////////////////////////////////////////////
	// Common

	struct Type
	{
		static const uint8_t i32 = 0x7F;
		static const uint8_t i64 = 0x7E;
		static const uint8_t f32 = 0x7D;
		static const uint8_t f64 = 0x7C;

		static const uint8_t s_Base = 0x7C; // for the 2-bit type encoding

		static uint8_t SizeOf(uint8_t t) {
			switch (t) {
			default:
				Fail();
			case i32:
			case f32:
				return 4;
			case i64:
			case f64:
				return 8;
			}
		}
	};


#define WasmInstructions_unop_i32_i32(macro) \
	macro(0x45, i32_eqz) \
	/*macro(0x69, i32_popcnt)*/ \

#define WasmInstructions_unop_i32_i64(macro) \
	macro(0x50, i64_eqz) \

#define WasmInstructions_binop_i32_i32(macro) \
	macro(0x46, i32_eq) \
	macro(0x47, i32_ne) \
	macro(0x48, i32_lt_s) \
	macro(0x49, i32_lt_u) \
	macro(0x4A, i32_gt_s) \
	macro(0x4B, i32_gt_u) \
	macro(0x4C, i32_le_s) \
	macro(0x4D, i32_le_u) \
	macro(0x4E, i32_ge_s) \
	macro(0x4F, i32_ge_u) \
	macro(0x6A, i32_add) \
	macro(0x6B, i32_sub) \
	macro(0x6C, i32_mul) \
	macro(0x6D, i32_div_s) \
	macro(0x6E, i32_div_u) \
	macro(0x6F, i32_rem_s) \
	macro(0x70, i32_rem_u) \
	macro(0x71, i32_and) \
	macro(0x72, i32_or) \
	macro(0x73, i32_xor) \
	macro(0x74, i32_shl) \
	macro(0x75, i32_shr_s) \
	macro(0x76, i32_shr_u) \
	macro(0x77, i32_rotl) \
	macro(0x78, i32_rotr) \


#define WasmInstructions_binop_i32_i64(macro) \
	macro(0x51, i64_eq) \
	macro(0x52, i64_ne) \
	macro(0x53, i64_lt_s) \
	macro(0x54, i64_lt_u) \
	macro(0x55, i64_gt_s) \
	macro(0x56, i64_gt_u) \
	macro(0x57, i64_le_s) \
	macro(0x58, i64_le_u) \
	macro(0x59, i64_ge_s) \
	macro(0x5A, i64_ge_u) \

#define WasmInstructions_CustomPorted(macro) \
	macro(0x1A, drop) \
	macro(0x1B, select) \
	macro(0x20, local_get) \
	macro(0x21, local_set) \
	macro(0x22, local_tee) \
	macro(0x2C, i32_load8_s) \
	macro(0x2D, i32_load8_u) \
	macro(0x3A, i32_store8) \
	macro(0x10, call) \
	macro(0x0C, br) \
	macro(0x0D, br_if) \
	macro(0x41, i32_const) \

#define WasmInstructions_Proprietary(macro) \
	macro(0x07, ret) \
	macro(0x08, call_ext) \

#define WasmInstructions_NotPorted(macro) \
	macro(0x02, block) \
	macro(0x03, loop) \
	macro(0x0B, end_block) \

#define WasmInstructions_AllInitial(macro) \
	WasmInstructions_CustomPorted(macro) \
	WasmInstructions_NotPorted(macro) \
	WasmInstructions_unop_i32_i32(macro) \
	WasmInstructions_unop_i32_i64(macro) \
	WasmInstructions_binop_i32_i32(macro) \
	WasmInstructions_binop_i32_i64(macro) \

	enum Instruction
	{
#define THE_MACRO(id, name) name = id,
		WasmInstructions_AllInitial(THE_MACRO) \
		WasmInstructions_Proprietary(THE_MACRO)
#undef THE_MACRO

	};

	/////////////////////////////////////////////
	// Compiler
	uint32_t Compiler::PerFunction::Locals::get_Size() const
	{
		return m_v.empty() ? 0 : m_v.back().m_Pos + m_v.back().m_Size;
	}

	void Compiler::PerFunction::Locals::Add(uint8_t nType)
	{
		uint32_t nPos = get_Size();

		auto& v = m_v.emplace_back();
		v.m_Type = nType;
		v.m_Pos = nPos;
		v.m_Size = Type::SizeOf(nType);
	}

	struct CompilerPlus
		:public Compiler
	{
#define WasmParserSections(macro) \
		macro(1, Type) \
		macro(2, Import) \
		macro(3, Funcs) \
		macro(6, Global) \
		macro(7, Export) \
		macro(10, Code) \

#define THE_MACRO(id, name) void OnSection_##name(Reader&);
		WasmParserSections(THE_MACRO)
#undef THE_MACRO

		void ParsePlus(Reader);
	};

	void Compiler::Parse(const Reader& inp)
	{
		auto& c = Cast::Up<CompilerPlus>(*this);
		static_assert(sizeof(c) == sizeof(*this));
		c.ParsePlus(inp);

	}

	void CompilerPlus::ParsePlus(Reader inp)
	{
		static const uint8_t pMagic[] = { 0, 'a', 's', 'm' };
		Test(!memcmp(pMagic, inp.Consume(sizeof(pMagic)), sizeof(pMagic)));

		static const uint8_t pVer[] = { 1, 0, 0, 0 };
		Test(!memcmp(pVer, inp.Consume(sizeof(pVer)), sizeof(pVer)));

		for (uint8_t nPrevSection = 0; inp.m_p0 < inp.m_p1; )
		{
			auto nSection = inp.Read1();
			Test(!nPrevSection || !nSection || (nSection > nPrevSection));

			auto nLen = inp.Read<uint32_t>();

			Reader inpSection;
			inpSection.m_p0 = inp.Consume(nLen);
			inpSection.m_p1 = inpSection.m_p0 + nLen;

			switch (nSection)
			{
#define THE_MACRO(id, name) case id: OnSection_##name(inpSection); Test(inpSection.m_p0 == inpSection.m_p1); break;
			WasmParserSections(THE_MACRO)
#undef THE_MACRO

			}

			if (nSection)
				nPrevSection = nSection;
		}

		m_Labels.m_Items.resize(m_Functions.size()); // function labels
	}


	void CompilerPlus::OnSection_Type(Reader& inp)
	{
		auto nCount = inp.Read<uint32_t>();
		m_Types.resize(nCount);

		for (uint32_t i = 0; i < nCount; i++)
		{
			auto& x = m_Types[i];

			Test(inp.Read1() == 0x60);

			x.m_Args.Read(inp);
			x.m_Rets.Read(inp);

			Test(x.m_Rets.n <= 1);
		}
	}

	void CompilerPlus::OnSection_Import(Reader& inp)
	{
		auto nCount = inp.Read<uint32_t>();
		m_Imports.resize(nCount);

		for (uint32_t i = 0; i < nCount; i++)
		{
			auto& x = m_Imports[i];
			x.m_sMod.Read(inp);
			x.m_sName.Read(inp);

			Test(inp.Read1() == 0); // import function, other imports are not supported

			x.m_TypeIdx = inp.Read<uint32_t>();
			Test(x.m_TypeIdx < m_Types.size());
		}
	}

	void CompilerPlus::OnSection_Funcs(Reader& inp)
	{
		auto nCount = inp.Read<uint32_t>();
		m_Functions.resize(nCount);

		for (uint32_t i = 0; i < nCount; i++)
		{
			auto& x = m_Functions[i];
			ZeroObject(x.m_Expression);

			x.m_TypeIdx = inp.Read<uint32_t>();
			Test(x.m_TypeIdx < m_Types.size());
		}
	}

	void CompilerPlus::OnSection_Global(Reader& inp)
	{
		auto nCount = inp.Read<uint32_t>();
		m_Globals.resize(nCount);

		for (uint32_t i = 0; i < nCount; i++)
		{
			auto& x = m_Globals[i];

			x.m_Type = inp.Read1();
			x.m_Mutable = inp.Read1();

			Fail(); // TODO: init expresssion
		}
	}

	void CompilerPlus::OnSection_Export(Reader& inp)
	{
		auto nCount = inp.Read<uint32_t>();
		m_Exports.resize(nCount);

		for (uint32_t i = 0; i < nCount; i++)
		{
			auto& x = m_Exports[i];
			x.m_sName.Read(inp);
			x.m_Kind = inp.Read1();
			x.m_Idx = inp.Read<uint32_t>();

			if (!x.m_Kind)
			{
				x.m_Idx -= static_cast<uint32_t>(m_Imports.size());
				Test(x.m_Idx < m_Functions.size());
			}
		}
	}

	void CompilerPlus::OnSection_Code(Reader& inp)
	{
		auto nCount = inp.Read<uint32_t>();
		Test(nCount == m_Functions.size());

		for (uint32_t i = 0; i < nCount; i++)
		{
			auto& x = m_Functions[i];

			auto nSize = inp.Read<uint32_t>();
			Reader inpFunc;
			inpFunc.m_p0 = inp.Consume(nSize);
			inpFunc.m_p1 = inpFunc.m_p0 + nSize;

			const auto& funcType = m_Types[x.m_TypeIdx];

			for (uint32_t iArg = 0; iArg < funcType.m_Args.n; iArg++)
				x.m_Locals.Add(funcType.m_Args.p[iArg]);

			auto nBlocks = inpFunc.Read<uint32_t>();
			for (uint32_t iBlock = 0; iBlock < nBlocks; iBlock++)
			{
				auto nVarsCount = inpFunc.Read<uint32_t>();
				uint8_t nType = inpFunc.Read1();

				while (nVarsCount--)
					x.m_Locals.Add(nType);
			}

			// the rest is the function body
			x.m_Expression = inpFunc;
		}
	}






	struct Compiler::Context
	{

		Compiler& m_This;
		const uint8_t* m_p0;
		uint32_t m_iFunc = 0;
		Reader m_Code;

		struct Block
		{
			PerType m_Type;
			size_t m_OperandsAtExit;
			uint32_t m_iLabel;
			bool m_Loop = false;
		};

		std::vector<Block> m_Blocks;
		std::vector<uint8_t> m_Operands;
		uint32_t m_SizeOperands = 0;


		Block& get_B() {
			Test(!m_Blocks.empty());
			return m_Blocks.back();
		}

		void Push(uint8_t x) {
			m_Operands.push_back(x);
			m_SizeOperands += Type::SizeOf(x);
		}

		uint8_t Pop() {
			Test(!m_Operands.empty());
			uint8_t ret = m_Operands.back();
			m_Operands.pop_back();

			m_SizeOperands -= Type::SizeOf(ret);
			return ret;
		}

		void Pop(uint8_t nType) {
			uint8_t x = Pop();
			Test(x == nType);
		}

		void TestOperands(const Vec<uint8_t>& v)
		{
			if (!v.n)
				return;

			Test(m_Operands.size() >= v.n);
			Test(!memcmp(&m_Operands.front() + m_Operands.size() - v.n, v.p, sizeof(*v.p) * v.n));
		}

		void BlockOpen(const PerType& tp)
		{
			auto& b = m_Blocks.emplace_back();
			b.m_OperandsAtExit = m_Operands.size();

			if (1 != m_Blocks.size())
			{
				TestOperands(tp.m_Args); // for most outer function block the args are not on the stack
				b.m_OperandsAtExit -= tp.m_Args.n;

				b.m_iLabel = static_cast<uint32_t>(m_This.m_Labels.m_Items.size());
				m_This.m_Labels.m_Items.push_back(0);
			}

			b.m_OperandsAtExit += tp.m_Rets.n;
			b.m_Type = tp;
		}

		void BlockOpen()
		{
			auto nType = m_Code.Read<uint32_t>();
			Test(0x40 == nType);
			PerType tp = { { 0 } };
			BlockOpen(tp);

			m_p0 = nullptr; // don't write
		}

		void TestBlockCanClose()
		{
			Test(m_Operands.size() == get_B().m_OperandsAtExit);
			TestOperands(get_B().m_Type.m_Rets);
		}

		void UpdTopBlockLabel()
		{
			m_This.m_Labels.m_Items[m_Blocks.back().m_iLabel] = static_cast<uint32_t>(m_This.m_Result.size());
		}

		static uint32_t SizeOfVars(const Vec<uint8_t>& v)
		{
			uint32_t nSize = 0;
			for (uint32_t i = 0; i < v.n; i++)
				nSize += Type::SizeOf(v.p[i]);
			return nSize;
		}

		void WriteRet()
		{
			WriteRes(static_cast<uint8_t>(Instruction::ret));

			// stack layout
			// ...
			// args
			// retaddr (4 bytes)
			// locals
			// retval

			// to allow for correct return we need to provide the following:

			uint32_t nSizeLocal = m_This.m_Functions[m_iFunc].m_Locals.get_Size(); // sizeof(args) + sizeof(locals)

			const auto& tp = get_B().m_Type;
			uint32_t nSizeArgs = SizeOfVars(tp.m_Args);

			WriteResU(SizeOfVars(tp.m_Rets) >> 2); // sizeof(retval)
			WriteResU((nSizeLocal - nSizeArgs) >> 2); // sizeof(locals), excluding args
			WriteResU(nSizeArgs >> 2);
		}

		void BlockClose()
		{
			TestBlockCanClose();

			if (1 == m_Blocks.size())
				WriteRet(); // end of function
			else
			{
				if (!m_Blocks.back().m_Loop)
					UpdTopBlockLabel();
			}

			m_Blocks.pop_back();

			m_p0 = nullptr; // don't write
		}

		void PutLabelTrg(uint32_t iLabel)
		{
			auto& lbl = m_This.m_Labels.m_Targets.emplace_back();
			lbl.m_iItem = iLabel;
			lbl.m_Pos = static_cast<uint32_t>(m_This.m_Result.size());

			uintBigFor<uint32_t>::Type offs(Zero);
			WriteRes(offs.m_pData, offs.nBytes);
		}

		void OnBranch()
		{
			auto nLabel = m_Code.Read<uint32_t>();
			Test(nLabel + 1 < m_Blocks.size());
			assert(nLabel < m_This.m_Labels.m_Items.size());

			auto& b = get_B();
			if (b.m_Loop)
			{
				assert(m_Blocks.size() > 1); // function block can't be loop

				size_t n = b.m_OperandsAtExit + b.m_Type.m_Args.n - b.m_Type.m_Rets.n;
				Test(m_Operands.size() == n);
				TestOperands(b.m_Type.m_Args);
			}
			else
			{
				TestBlockCanClose();
			}

			WriteRes(*m_p0); // opcode
			m_p0 = nullptr;

			PutLabelTrg(m_Blocks[m_Blocks.size() - (nLabel + 1)].m_iLabel);
		}

		uint8_t OnLocalVar()
		{
			WriteInstruction();

			const auto& f = m_This.m_Functions[m_iFunc];
			auto iVar = m_Code.Read<uint32_t>();

			Test(iVar < f.m_Locals.m_v.size());
			const auto var = f.m_Locals.m_v[iVar];


			// add type and position of the local wrt current stack

			// stack layout
			// ...
			// args
			// retaddr (4 bytes)
			// locals
			// current operand stack

			uint32_t nOffs = m_SizeOperands + f.m_Locals.get_Size() - var.m_Pos;

			const auto& fType = m_This.m_Types[f.m_TypeIdx];
			if (iVar < fType.m_Args.n)
				nOffs += sizeof(uint32_t); // retaddr

			assert(!(nOffs & 3));
			assert(var.m_Type - Type::s_Base <= 3);;
			nOffs |= (var.m_Type - Type::s_Base);

			WriteResU(nOffs);

			return var.m_Type;
		}

		void On_local_get() {
			Push(OnLocalVar());
		}
		void On_local_set() {
			Pop(OnLocalVar());
		}

		void On_local_tee() {
			auto nType = OnLocalVar();
			Pop(nType);
			Push(nType);
		}

		void On_drop() {
			WriteInstruction();
			WriteRes(Pop());
		}

		void On_select() {
			WriteInstruction();

			Pop(Type::i32);
			uint8_t nType = Pop();
			Pop(nType); // must be the same

			WriteRes(nType);
		}

		void On_i32_load8() {
			auto nPad = m_Code.Read<uint32_t>();
			auto nOffs = m_Code.Read<uint32_t>();
			nOffs;
			nPad;

			Pop(Type::i32);
			Push(Type::i32);
		}

		void On_i32_load8_u() {
			On_i32_load8();
		}
		void On_i32_load8_s() {
			On_i32_load8();
		}

		void On_i32_store8() {
			auto nPad = m_Code.Read<uint32_t>();
			auto nOffs = m_Code.Read<uint32_t>();
			nOffs;
			nPad;

			Pop(Type::i32);
			Pop(Type::i32);
		}

		void On_block() {
			BlockOpen();
		}

		void On_loop() {
			BlockOpen();
			get_B().m_Loop = true;
			UpdTopBlockLabel();
		}

		void On_end_block() {
			BlockClose();
		}

		void On_br() {
			OnBranch();
		}

		void On_br_if() {
			Pop(Type::i32); // conditional
			OnBranch();
		}

		void On_i32_const() {
			auto val = m_Code.Read<uint32_t>();
			val;
			Push(Type::i32);
		}

		void On_call()
		{
			auto iFunc = m_Code.Read<uint32_t>();

			bool bImported = (iFunc < m_This.m_Imports.size());
			if (!bImported)
			{
				iFunc -= static_cast<uint32_t>(m_This.m_Imports.size());
				Test(iFunc < m_This.m_Functions.size());
			}

			uint32_t iTypeIdx = bImported ? m_This.m_Imports[iFunc].m_TypeIdx : m_This.m_Functions[iFunc].m_TypeIdx;
			const auto& tp = m_This.m_Types[iTypeIdx];

			for (uint32_t i = tp.m_Args.n; i--; )
				Pop(tp.m_Args.p[i]);

			for (uint32_t i = 0; i < tp.m_Rets.n; i++)
				Push(tp.m_Rets.p[i]);

			m_p0 = nullptr; // don't write

			if (bImported)
			{
				WriteRes(static_cast<uint8_t>(Instruction::call_ext));
				WriteResU(m_This.m_Imports[iFunc].m_Binding);
			}
			else
			{
				WriteRes(static_cast<uint8_t>(Instruction::call));
				PutLabelTrg(iFunc);
			}
		}

		void CompileFunc();


		Context(Compiler& x) :m_This(x) {}

		void WriteRes(uint8_t x) {
			m_This.m_Result.push_back(x);
		}

		void Write(Instruction x) {
			WriteRes(static_cast<uint8_t>(x));
		}

		void WriteRes(const uint8_t* p, uint32_t n);
		void WriteResU(uint64_t);
		void WriteResS(int64_t);

		void WriteInstruction();
	};

	void Compiler::Context::WriteInstruction()
	{
		if (m_p0)
		{
			WriteRes(m_p0, static_cast<uint32_t>(m_Code.m_p0 - m_p0));
			m_p0 = nullptr;
		}
	}

	void Compiler::Context::WriteRes(const uint8_t* p, uint32_t n)
	{
		if (n)
		{
			size_t n0 = m_This.m_Result.size();
			m_This.m_Result.resize(n0 + n);
			memcpy(&m_This.m_Result.front() + n0, p, n);
		}
	}

	void Compiler::Context::WriteResU(uint64_t x)
	{
		while (true)
		{
			uint8_t n = static_cast<uint8_t>(x);
			x >>= 7;

			if (!x)
			{
				assert(!(n & 0x80));
				WriteRes(n);
				break;
			}

			WriteRes(n | 0x80);
		}
	}

	void Compiler::Context::WriteResS(int64_t x)
	{
		while (true)
		{
			uint8_t n = static_cast<uint8_t>(x);
			x >>= 6; // sign bit is propagated

			if (!x)
			{
				assert((n & 0xC0) == 0);
				WriteRes(n);
				break;
			}

			if (-1 == x)
			{
				assert((n & 0xC0) == 0xC0);
				WriteRes(n & ~0x80); // sign bit must remain
				break;
			}

			WriteRes(n | 0x80);
			x >>= 1;
		}
	}



	void Compiler::Build()
	{
		for (uint32_t i = 0; i < m_Functions.size(); i++)
		{
			Context ctx(*this);
			m_Labels.m_Items[i] = static_cast<uint32_t>(m_Result.size());
			ctx.m_iFunc = i;
			ctx.CompileFunc();
		}

		for (uint32_t i = 0; i < m_Labels.m_Targets.size(); i++)
		{
			auto& trg = m_Labels.m_Targets[i];

			uintBigFor<uint32_t>::Type nPos = m_Labels.m_Items[trg.m_iItem];
			memcpy(&m_Result.front() + trg.m_Pos, nPos.m_pData, nPos.nBytes);
		}

	}



	void Compiler::Context::CompileFunc()
	{
		auto& func = m_This.m_Functions[m_iFunc];

		m_Code = func.m_Expression;
		BlockOpen(m_This.m_Types[func.m_TypeIdx]);

		for (uint32_t nLine = 0; !m_Blocks.empty(); nLine++)
		{
			nLine; // for dbg
			m_p0 = m_Code.m_p0;

			typedef Instruction I;
			I nInstruction = (I) m_Code.Read1();

			switch (nInstruction)
			{
#define THE_MACRO(id, name) \
			case I::name: \
				On_##name(); \
				break;

			WasmInstructions_CustomPorted(THE_MACRO)
			WasmInstructions_NotPorted(THE_MACRO)
#undef THE_MACRO

#define THE_MACRO(id, name) case id:
			WasmInstructions_unop_i32_i32(THE_MACRO)
#undef THE_MACRO
			{
				Pop(Type::i32);
				Push(Type::i32);

			} break;

#define THE_MACRO(id, name) case id:
			WasmInstructions_unop_i32_i64(THE_MACRO)
#undef THE_MACRO
			{
				Pop(Type::i64);
				Push(Type::i32);

			} break;

#define THE_MACRO(id, name) case id:
			WasmInstructions_binop_i32_i32(THE_MACRO)
#undef THE_MACRO
			{
				Pop(Type::i32);
				Pop(Type::i32);
				Push(Type::i32);

			} break;

#define THE_MACRO(id, name) case id:
			WasmInstructions_binop_i32_i64(THE_MACRO)
#undef THE_MACRO
			{
				Pop(Type::i64);
				Pop(Type::i64);
				Push(Type::i32);

			} break;

			default:
				Fail();
			}

			WriteInstruction(); // unless already written
		}

		Test(m_Code.m_p0 == m_Code.m_p1);

	}




	/////////////////////////////////////////////
	// Processor


	struct ProcessorPlus
		:public Processor
	{

#define THE_MACRO_unop(tout, tin, name) \
		void On_##name() \
		{ \
			Push<tout>(Eval_##name(*Pop<tin>())); \
		}

#define THE_MACRO_binop(tout, tin, name) \
		void On_##name() \
		{ \
			const tin* pB = Pop<tin>(); \
			const tin* pA = Pop<tin>(); \
			Push<tout>(Eval_##name(*pA, *pB)); \
		}

#define THE_MACRO(id, name) THE_MACRO_unop(uint32_t, uint32_t, name)
		WasmInstructions_unop_i32_i32(THE_MACRO)
#undef THE_MACRO

#define THE_MACRO(id, name) THE_MACRO_unop(uint32_t, uint64_t, name)
		WasmInstructions_unop_i32_i64(THE_MACRO)
#undef THE_MACRO

#define THE_MACRO(id, name) THE_MACRO_binop(uint32_t, uint32_t, name)
		WasmInstructions_binop_i32_i32(THE_MACRO)
#undef THE_MACRO

#define THE_MACRO(id, name) THE_MACRO_binop(uint32_t, uint64_t, name)
		WasmInstructions_binop_i32_i64(THE_MACRO)
#undef THE_MACRO

#define THE_MACRO(id, name) void On_##name();
		WasmInstructions_CustomPorted(THE_MACRO)
		WasmInstructions_Proprietary(THE_MACRO)
#undef THE_MACRO

#define UNOP(tout, tin, name) static tout Eval_##name(const tin& x)
#define BINOP(tout, tin, name) static tout Eval_##name(const tin& a, const tin& b)


		UNOP(uint32_t, uint32_t, i32_eqz) { return x == 0; }
		// UNOP(uint32_t, uint32_t, i32_popcnt) { return __popcnt(x); }
		BINOP(uint32_t, uint32_t, i32_eq) { return a == b; }
		BINOP(uint32_t, uint32_t, i32_ne) { return a != b; }
		BINOP(uint32_t, uint32_t, i32_lt_s) { return static_cast<int32_t>(a) < static_cast<int32_t>(b); }
		BINOP(uint32_t, uint32_t, i32_gt_s) { return static_cast<int32_t>(a) > static_cast<int32_t>(b); }
		BINOP(uint32_t, uint32_t, i32_le_s) { return static_cast<int32_t>(a) <= static_cast<int32_t>(b); }
		BINOP(uint32_t, uint32_t, i32_ge_s) { return static_cast<int32_t>(a) >= static_cast<int32_t>(b); }
		BINOP(uint32_t, uint32_t, i32_lt_u) { return a < b; }
		BINOP(uint32_t, uint32_t, i32_gt_u) { return a > b; }
		BINOP(uint32_t, uint32_t, i32_le_u) { return a <= b; }
		BINOP(uint32_t, uint32_t, i32_ge_u) { return a >= b; }
		BINOP(uint32_t, uint32_t, i32_add) { return a + b; }
		BINOP(uint32_t, uint32_t, i32_sub) { return a - b; }
		BINOP(uint32_t, uint32_t, i32_mul) { return a * b; }
		BINOP(uint32_t, uint32_t, i32_div_s) { Test(b);  return static_cast<int32_t>(a) / static_cast<int32_t>(b); }
		BINOP(uint32_t, uint32_t, i32_div_u) { Test(b);  return a / b; }
		BINOP(uint32_t, uint32_t, i32_rem_s) { Test(b);  return static_cast<int32_t>(a) % static_cast<int32_t>(b); }
		BINOP(uint32_t, uint32_t, i32_rem_u) { Test(b);  return a % b; }
		BINOP(uint32_t, uint32_t, i32_and) { return a & b; }
		BINOP(uint32_t, uint32_t, i32_or) { return a | b; }
		BINOP(uint32_t, uint32_t, i32_xor) { return a ^ b; }
		BINOP(uint32_t, uint32_t, i32_shl) { Test(b < 32); return a << b; }
		BINOP(uint32_t, uint32_t, i32_shr_s) { Test(b < 32); return static_cast<int32_t>(a) >> b; }
		BINOP(uint32_t, uint32_t, i32_shr_u) { Test(b < 32); return a >> b; }
		BINOP(uint32_t, uint32_t, i32_rotl) { Test(b < 32); if (!b) return a; return (a << b) | (a >> (32-b)); }
		BINOP(uint32_t, uint32_t, i32_rotr) { Test(b < 32); if (!b) return a; return (a >> b) | (a << (32 - b)); }

		UNOP(uint32_t, uint64_t, i64_eqz) { return x == 0; }
		BINOP(uint32_t, uint64_t, i64_eq) { return a == b; }
		BINOP(uint32_t, uint64_t, i64_ne) { return a != b; }
		BINOP(uint32_t, uint64_t, i64_lt_s) { return static_cast<int64_t>(a) < static_cast<int64_t>(b); }
		BINOP(uint32_t, uint64_t, i64_gt_s) { return static_cast<int64_t>(a) > static_cast<int64_t>(b); }
		BINOP(uint32_t, uint64_t, i64_le_s) { return static_cast<int64_t>(a) <= static_cast<int64_t>(b); }
		BINOP(uint32_t, uint64_t, i64_ge_s) { return static_cast<int64_t>(a) >= static_cast<int64_t>(b); }
		BINOP(uint32_t, uint64_t, i64_lt_u) { return a < b; }
		BINOP(uint32_t, uint64_t, i64_gt_u) { return a > b; }
		BINOP(uint32_t, uint64_t, i64_le_u) { return a <= b; }
		BINOP(uint32_t, uint64_t, i64_ge_u) { return a >= b; }


		uint32_t ReadAddr()
		{
			uint32_t x;
			typedef uintBigFor<uint32_t>::Type AddrType;
			((AddrType*) m_Instruction.Consume(sizeof(AddrType)))->Export(x);
			return x;
		}

		void OnLocal(bool bSet, bool bGet)
		{
			uint32_t nOffset = m_Instruction.Read<uint32_t>();

			uint8_t nType = Type::s_Base + static_cast<uint8_t>(3 & (nOffset - Type::s_Base));
			uint8_t nSize = Type::SizeOf(nType);

			nOffset >>= 2;
			nSize >>= 2;

			Test((nOffset >= nSize) && (nOffset <= m_Sp));

			uint32_t* pDst = m_pStack + m_Sp - nOffset;
			uint32_t* pSrc = m_pStack + m_Sp;

			if (!bSet)
			{
				std::swap(pDst, pSrc);
				m_Sp += nSize;
				Test(m_Sp <= _countof(m_pStack));
			}
			else
			{
				pSrc -= nSize;

				if (!bGet)
					m_Sp -= nSize;
			}

			for (uint32_t i = 0; i < nSize; i++)
				pDst[i] = pSrc[i];
		}

		uint8_t* get_LinearAddr(uint32_t nOffset, uint32_t nSize)
		{
			Test(nOffset + nSize >= nSize); // no overflow
			Test(nOffset + nSize < _countof(m_pLinearMem));
			return m_pLinearMem + nOffset;
		}

		uint8_t* On_Load(uint32_t nSize)
		{
			auto nPad = m_Instruction.Read<uint32_t>(); nPad;
			auto nOffs = m_Instruction.Read<uint32_t>();
			nOffs += *Pop<uint32_t>();

			return get_LinearAddr(nOffs, nSize);
		}



		void RunOncePlus()
		{
			typedef Instruction I;
			I nInstruction = (I) m_Instruction.Read1();

			switch (nInstruction)
			{
#define THE_MACRO(id, name) case I::name: On_##name(); break;

			WasmInstructions_unop_i32_i32(THE_MACRO)
			WasmInstructions_unop_i32_i64(THE_MACRO)
			WasmInstructions_binop_i32_i32(THE_MACRO)
			WasmInstructions_binop_i32_i64(THE_MACRO)
			WasmInstructions_CustomPorted(THE_MACRO)
			WasmInstructions_Proprietary(THE_MACRO)
#undef THE_MACRO


			default:
				Fail();
			}

		}

	};

	void Processor::RunOnce()
	{
		auto& p = Cast::Up<ProcessorPlus>(*this);
		static_assert(sizeof(p) == sizeof(*this));
		p.RunOncePlus();
	}

	void Processor::InvokeExt(uint32_t)
	{
		Fail(); // unresolved binding
	}

	void Processor::Jmp(uint32_t ip)
	{
		Test(ip < m_Code.n);

		m_Instruction.m_p0 = reinterpret_cast<const uint8_t*>(m_Code.p) + ip;
		m_Instruction.m_p1 = m_Instruction.m_p0 + m_Code.n - ip;
	}

	void ProcessorPlus::On_local_get()
	{
		OnLocal(false, true);
	}

	void ProcessorPlus::On_local_set()
	{
		OnLocal(true, false);
	}

	void ProcessorPlus::On_local_tee()
	{
		OnLocal(true, true);
	}

	void ProcessorPlus::On_drop()
	{
		uint32_t nSize = Type::SizeOf(m_Instruction.Read1()) >> 2;
		Test(m_Sp >= nSize);
		m_Sp -= nSize;
	}

	void ProcessorPlus::On_select()
	{
		uint32_t nSize = Type::SizeOf(m_Instruction.Read1()) >> 2;
		auto nSel = Pop<uint32_t>();

		Test(m_Sp >= (nSize << 1)); // must be at least 2 such operands
		m_Sp -= nSize;

		if (!nSel)
		{
			for (uint32_t i = 0; i < nSize; i++)
				m_pStack[m_Sp + i - nSize] = m_pStack[m_Sp + i];
		}
	}

	void ProcessorPlus::On_i32_load8_u()
	{
		uint32_t val = *On_Load(1);
		Push(val);
	}

	void ProcessorPlus::On_i32_load8_s()
	{
		char ch = *On_Load(1);
		int32_t val = ch; // promoted w.r.t. sign
		Push<uint32_t>(val);
	}

	void ProcessorPlus::On_i32_store8()
	{
		uint32_t val = *Pop<uint32_t>();
		auto nPad = m_Instruction.Read<uint32_t>(); nPad;
		auto nOffs = m_Instruction.Read<uint32_t>();
		nOffs += *Pop<uint32_t>();

		Test(nOffs < _countof(m_pLinearMem));
		m_pLinearMem[nOffs] = static_cast<uint8_t>(val);
	}

	void ProcessorPlus::On_br()
	{
		Jmp(ReadAddr());
	}

	void ProcessorPlus::On_br_if()
	{
		uint32_t addr = ReadAddr();
		if (*Pop<uint32_t>())
			Jmp(addr);
	}

	void ProcessorPlus::On_call()
	{
		uint32_t nAddr = ReadAddr();
		uint32_t nRetAddr = static_cast<uint32_t>(m_Instruction.m_p0 - (const uint8_t*) m_Code.p);
		Push(nRetAddr);
		Jmp(nAddr);
	}

	void ProcessorPlus::On_call_ext()
	{
		InvokeExt(m_Instruction.Read<uint32_t>());
	}


	void ProcessorPlus::On_i32_const()
	{
		Push<int32_t>(m_Instruction.Read<int32_t>());
	}

	void ProcessorPlus::On_ret()
	{
		auto nRets = m_Instruction.Read<uint32_t>();
		auto nLocals = m_Instruction.Read<uint32_t>();
		auto nArgs = m_Instruction.Read<uint32_t>();

		// stack layout
		// ...
		// args
		// retaddr (4 bytes)
		// locals
		// retval

		uint32_t nPosRetSrc = m_Sp - nRets;
		Test(nPosRetSrc <= m_Sp);

		uint32_t nPosAddr = nPosRetSrc - (nLocals + 1);
		Test(nPosAddr < nPosRetSrc);

		uint32_t nPosRetDst = nPosAddr - nArgs;
		Test(nPosRetDst <= nPosAddr);

		uint32_t nRetAddr = m_pStack[nPosAddr];

		for (uint32_t i = 0; i < nRets; i++)
			m_pStack[nPosRetDst + i] = m_pStack[nPosRetSrc + i];


		m_Sp = nPosRetDst + nRets;
		Jmp(nRetAddr);
	}





} // namespace Wasm
} // namespace beam
