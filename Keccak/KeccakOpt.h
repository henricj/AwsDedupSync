// KeccakOpt.h

#pragma once

using namespace System;

extern "C"  {
#include "KeccakHash.h"
#include "KeccakF-1600-interface.h"
}

namespace KeccakOpt {

    public ref class Keccak : System::Security::Cryptography::HashAlgorithm
    {
        // TODO: Add your methods for this class here.
        AlignedAutoPtr<Keccak_HashInstance> hash_;
        static Keccak();
    public:
        static void Validate();
        void Initialize() override;
        void HashCore(cli::array<unsigned char> ^ buffer, int offset, int length) override;
        cli::array<unsigned char> ^ HashFinal() override;
    };
}
