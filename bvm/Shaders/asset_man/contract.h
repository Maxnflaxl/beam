#pragma once

namespace AssetMan
{
    static const ShaderID s_SID = { 0x95,0x9b,0x36,0x28,0x28,0x3b,0x83,0x74,0x79,0x57,0xe8,0x5f,0x1e,0xe8,0xa6,0x6d,0xee,0xc9,0x33,0x48,0x7b,0xe2,0xff,0x08,0xdb,0x39,0x2d,0xc5,0x1c,0x9c,0xc0,0xb4 };

#pragma pack (push, 1)

    namespace Method
    {
        struct AssetReg
        {
            static const uint32_t s_iMethod = 2;
            uint32_t m_SizeMetadata;
            // followed by metadata
        };

        struct AssetUnreg
        {
            static const uint32_t s_iMethod = 3;
            AssetID m_Aid;
        };
    }


#pragma pack (pop)

}
