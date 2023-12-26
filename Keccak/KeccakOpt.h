// KeccakOpt.h

#pragma once

using namespace System;

extern "C" {
#include "KeccakHash.h"
}

namespace KeccakOpt
{
public ref class Keccak sealed
{
    // TODO: Add your methods for this class here.
    AlignedAutoPtr<Keccak_HashInstance> hash_;
    static Keccak();

public:
    const static int HashSizeInBytes = 64;

    Keccak();
    ~Keccak();

    static void Validate();
    void AppendData(ReadOnlySpan<unsigned char> data);
    void Initialize();
    void GetHashAndReset(Span<unsigned char> hash);
};
}
