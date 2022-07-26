// This is the main DLL file.

#include "stdafx.h"

#include "KeccakOpt.h"


namespace KeccakOpt
{
#pragma unmanaged

static const unsigned char Hash0[] =
{
    0xA6, 0x9F, 0x73, 0xCC, 0xA2, 0x3A, 0x9A, 0xC5, 0xC8, 0xB5, 0x67, 0xDC, 0x18, 0x5A, 0x75, 0x6E,
    0x97, 0xC9, 0x82, 0x16, 0x4F, 0xE2, 0x58, 0x59, 0xE0, 0xD1, 0xDC, 0xC1, 0x47, 0x5C, 0x80, 0xA6,
    0x15, 0xB2, 0x12, 0x3A, 0xF1, 0xF5, 0xF9, 0x4C, 0x11, 0xE3, 0xE9, 0x40, 0x2C, 0x3A, 0xC5, 0x58,
    0xF5, 0x00, 0x19, 0x9D, 0x95, 0xB6, 0xD3, 0xE3, 0x01, 0x75, 0x85, 0x86, 0x28, 0x1D, 0xCD, 0x26
};

struct TestVector
{
    const int BitLength;
    const unsigned char* const Message;
    const unsigned char* const Hash;

    TestVector(int bitLength, const unsigned char* const message, const unsigned char* const hash)
        : BitLength(bitLength), Message(message), Hash(hash)
    { }
};

static const unsigned char Msg520[] =
{
    0x16, 0xE8, 0xB3, 0xD8, 0xF9, 0x88, 0xE9, 0xBB, 0x04, 0xDE, 0x9C, 0x96, 0xF2, 0x62, 0x78, 0x11,
    0xC9, 0x73, 0xCE, 0x4A, 0x52, 0x96, 0xB4, 0x77, 0x2C, 0xA3, 0xEE, 0xFE, 0xB8, 0x0A, 0x65, 0x2B,
    0xDF, 0x21, 0xF5, 0x0D, 0xF7, 0x9F, 0x32, 0xDB, 0x23, 0xF9, 0xF7, 0x3D, 0x39, 0x3B, 0x2D, 0x57,
    0xD9, 0xA0, 0x29, 0x7F, 0x7A, 0x2F, 0x2E, 0x79, 0xCF, 0xDA, 0x39, 0xFA, 0x39, 0x3D, 0xF1, 0xAC,
    0x00,
};

static const unsigned char Hash520[] =
{
    0xB0, 0xE2, 0x3D, 0x60, 0x0B, 0xA4, 0x21, 0x5F, 0x79, 0xD5, 0x00, 0x47, 0xBB, 0xFE, 0xD5, 0x0D,
    0xF7, 0xD6, 0xE7, 0x69, 0x51, 0x4D, 0x79, 0x6A, 0xFD, 0x16, 0x6D, 0xEE, 0xCA, 0x88, 0xBD, 0x1C,
    0xBE, 0x0A, 0xFC, 0x72, 0xA4, 0x1E, 0x03, 0x17, 0xA2, 0x23, 0x22, 0x5B, 0x4F, 0x58, 0x82, 0xF7,
    0x23, 0xAF, 0xCB, 0xA3, 0xAF, 0x7C, 0x45, 0x7E, 0xB5, 0x25, 0x94, 0x6D, 0xA6, 0xC5, 0x3B, 0xB0,
};

static const unsigned char Msg1032[] =
{
    0x10, 0xDB, 0x50, 0x9B, 0x2C, 0xDC, 0xAB, 0xA6, 0xC0, 0x62, 0xAE, 0x33, 0xBE, 0x48, 0x11, 0x6A,
    0x29, 0xEB, 0x18, 0xE3, 0x90, 0xE1, 0xBB, 0xAD, 0xA5, 0xCA, 0x0A, 0x27, 0x18, 0xAF, 0xBC, 0xD2,
    0x34, 0x31, 0x44, 0x01, 0x06, 0x59, 0x48, 0x93, 0x04, 0x3C, 0xC7, 0xF2, 0x62, 0x52, 0x81, 0xBF,
    0x7D, 0xE2, 0x65, 0x58, 0x80, 0x96, 0x6A, 0x23, 0x70, 0x5F, 0x0C, 0x51, 0x55, 0xC2, 0xF5, 0xCC,
    0xA9, 0xF2, 0xC2, 0x14, 0x2E, 0x96, 0xD0, 0xA2, 0xE7, 0x63, 0xB7, 0x06, 0x86, 0xCD, 0x42, 0x1B,
    0x5D, 0xB8, 0x12, 0xDA, 0xCE, 0xD0, 0xC6, 0xD6, 0x50, 0x35, 0xFD, 0xE5, 0x58, 0xE9, 0x4F, 0x26,
    0xB3, 0xE6, 0xDD, 0xE5, 0xBD, 0x13, 0x98, 0x0C, 0xC8, 0x02, 0x92, 0xB7, 0x23, 0x01, 0x3B, 0xD0,
    0x33, 0x28, 0x45, 0x84, 0xBF, 0xF2, 0x76, 0x57, 0x87, 0x1B, 0x0C, 0xF0, 0x7A, 0x84, 0x9F, 0x4A,
    0xE2,
};

static const unsigned char Hash1032[] =
{
    0x5F, 0x0B, 0xFB, 0x41, 0x46, 0x91, 0x0C, 0xF0, 0xC3, 0x20, 0x36, 0x4B, 0x6A, 0xD8, 0xA0, 0x2B,
    0x09, 0x66, 0x22, 0x9A, 0xB2, 0x67, 0x6D, 0x96, 0x70, 0xF0, 0xDD, 0x24, 0x1E, 0x81, 0x04, 0xDB,
    0x02, 0x79, 0x7E, 0xEF, 0xEA, 0x0B, 0x9C, 0xAB, 0xBE, 0x90, 0xA4, 0x47, 0x57, 0xB0, 0x33, 0x75,
    0x59, 0x25, 0xB2, 0xFC, 0xCF, 0x3A, 0x00, 0x05, 0x4F, 0x9A, 0xE8, 0xFB, 0xCE, 0xF7, 0x52, 0xA8,
};

static const unsigned char Msg1264[] =
{
    0xF7, 0x6B, 0x85, 0xDC, 0x67, 0x42, 0x10, 0x25, 0xD6, 0x4E, 0x93, 0x09, 0x6D, 0x1D, 0x71, 0x2B,
    0x7B, 0xAF, 0x7F, 0xB0, 0x01, 0x71, 0x6F, 0x02, 0xD3, 0x3B, 0x21, 0x60, 0xC2, 0xC8, 0x82, 0xC3,
    0x10, 0xEF, 0x13, 0xA5, 0x76, 0xB1, 0xC2, 0xD3, 0x0E, 0xF8, 0xF7, 0x8E, 0xF8, 0xD2, 0xF4, 0x65,
    0x00, 0x71, 0x09, 0xAA, 0xD9, 0x3F, 0x74, 0xCB, 0x9E, 0x7D, 0x7B, 0xEF, 0x7C, 0x95, 0x90, 0xE8,
    0xAF, 0x3B, 0x26, 0x7C, 0x89, 0xC1, 0x5D, 0xB2, 0x38, 0x13, 0x8C, 0x45, 0x83, 0x3C, 0x98, 0xCC,
    0x4A, 0x47, 0x1A, 0x78, 0x02, 0x72, 0x3E, 0xF4, 0xC7, 0x44, 0xA8, 0x53, 0xCF, 0x80, 0xA0, 0xC2,
    0x56, 0x8D, 0xD4, 0xED, 0x58, 0xA2, 0xC9, 0x64, 0x48, 0x06, 0xF4, 0x21, 0x04, 0xCE, 0xE5, 0x36,
    0x28, 0xE5, 0xBD, 0xF7, 0xB6, 0x3B, 0x0B, 0x33, 0x8E, 0x93, 0x1E, 0x31, 0xB8, 0x7C, 0x24, 0xB1,
    0x46, 0xC6, 0xD0, 0x40, 0x60, 0x55, 0x67, 0xCE, 0xEF, 0x59, 0x60, 0xDF, 0x9E, 0x02, 0x2C, 0xB4,
    0x69, 0xD4, 0xC7, 0x87, 0xF4, 0xCB, 0xA3, 0xC5, 0x44, 0xA1, 0xAC, 0x91, 0xF9, 0x5F,
};

static const unsigned char Hash1264[] =
{
    0xD9, 0xA4, 0x27, 0x61, 0xF9, 0x80, 0xC7, 0x8C, 0x36, 0xCF, 0x54, 0xC4, 0x20, 0x7B, 0x0A, 0x62,
    0x95, 0x4E, 0x15, 0xA9, 0x07, 0xA7, 0xCE, 0xA1, 0x49, 0xB3, 0x7A, 0x4E, 0x0A, 0x63, 0x76, 0x20,
    0x2F, 0xF8, 0xF1, 0x2E, 0x16, 0xEB, 0xAD, 0x3A, 0xEC, 0xC7, 0xFF, 0x3A, 0x9D, 0x6A, 0xD0, 0x93,
    0xB0, 0x68, 0xDF, 0xE2, 0x72, 0xE3, 0xB9, 0x64, 0x6B, 0x1A, 0xED, 0xC0, 0x49, 0x61, 0xDC, 0x81,
};

static const unsigned char Msg2040[] =
{
    0x3A, 0x3A, 0x81, 0x9C, 0x48, 0xEF, 0xDE, 0x2A, 0xD9, 0x14, 0xFB, 0xF0, 0x0E, 0x18, 0xAB, 0x6B,
    0xC4, 0xF1, 0x45, 0x13, 0xAB, 0x27, 0xD0, 0xC1, 0x78, 0xA1, 0x88, 0xB6, 0x14, 0x31, 0xE7, 0xF5,
    0x62, 0x3C, 0xB6, 0x6B, 0x23, 0x34, 0x67, 0x75, 0xD3, 0x86, 0xB5, 0x0E, 0x98, 0x2C, 0x49, 0x3A,
    0xDB, 0xBF, 0xC5, 0x4B, 0x9A, 0x3C, 0xD3, 0x83, 0x38, 0x23, 0x36, 0xA1, 0xA0, 0xB2, 0x15, 0x0A,
    0x15, 0x35, 0x8F, 0x33, 0x6D, 0x03, 0xAE, 0x18, 0xF6, 0x66, 0xC7, 0x57, 0x3D, 0x55, 0xC4, 0xFD,
    0x18, 0x1C, 0x29, 0xE6, 0xCC, 0xFD, 0xE6, 0x3E, 0xA3, 0x5F, 0x0A, 0xDF, 0x58, 0x85, 0xCF, 0xC0,
    0xA3, 0xD8, 0x4A, 0x2B, 0x2E, 0x4D, 0xD2, 0x44, 0x96, 0xDB, 0x78, 0x9E, 0x66, 0x31, 0x70, 0xCE,
    0xF7, 0x47, 0x98, 0xAA, 0x1B, 0xBC, 0xD4, 0x57, 0x4E, 0xA0, 0xBB, 0xA4, 0x04, 0x89, 0xD7, 0x64,
    0xB2, 0xF8, 0x3A, 0xAD, 0xC6, 0x6B, 0x14, 0x8B, 0x4A, 0x0C, 0xD9, 0x52, 0x46, 0xC1, 0x27, 0xD5,
    0x87, 0x1C, 0x4F, 0x11, 0x41, 0x86, 0x90, 0xA5, 0xDD, 0xF0, 0x12, 0x46, 0xA0, 0xC8, 0x0A, 0x43,
    0xC7, 0x00, 0x88, 0xB6, 0x18, 0x36, 0x39, 0xDC, 0xFD, 0xA4, 0x12, 0x5B, 0xD1, 0x13, 0xA8, 0xF4,
    0x9E, 0xE2, 0x3E, 0xD3, 0x06, 0xFA, 0xAC, 0x57, 0x6C, 0x3F, 0xB0, 0xC1, 0xE2, 0x56, 0x67, 0x1D,
    0x81, 0x7F, 0xC2, 0x53, 0x4A, 0x52, 0xF5, 0xB4, 0x39, 0xF7, 0x2E, 0x42, 0x4D, 0xE3, 0x76, 0xF4,
    0xC5, 0x65, 0xCC, 0xA8, 0x23, 0x07, 0xDD, 0x9E, 0xF7, 0x6D, 0xA5, 0xB7, 0xC4, 0xEB, 0x7E, 0x08,
    0x51, 0x72, 0xE3, 0x28, 0x80, 0x7C, 0x02, 0xD0, 0x11, 0xFF, 0xBF, 0x33, 0x78, 0x53, 0x78, 0xD7,
    0x9D, 0xC2, 0x66, 0xF6, 0xA5, 0xBE, 0x6B, 0xB0, 0xE4, 0xA9, 0x2E, 0xCE, 0xEB, 0xAE, 0xB1,
};

static const unsigned char Hash2040[] =
{
    0x6E, 0x8B, 0x8B, 0xD1, 0x95, 0xBD, 0xD5, 0x60, 0x68, 0x9A, 0xF2, 0x34, 0x8B, 0xDC, 0x74, 0xAB,
    0x7C, 0xD0, 0x5E, 0xD8, 0xB9, 0xA5, 0x77, 0x11, 0xE9, 0xBE, 0x71, 0xE9, 0x72, 0x6F, 0xDA, 0x45,
    0x91, 0xFE, 0xE1, 0x22, 0x05, 0xED, 0xAC, 0xAF, 0x82, 0xFF, 0xBB, 0xAF, 0x16, 0xDF, 0xF9, 0xE7,
    0x02, 0xA7, 0x08, 0x86, 0x20, 0x80, 0x16, 0x6C, 0x2F, 0xF6, 0xBA, 0x37, 0x9B, 0xC7, 0xFF, 0xC2,
};

static const TestVector* const TestVectors[] =
{
    new TestVector(8 * sizeof(Msg520), Msg520, Hash520),
    new TestVector(8 * sizeof(Msg1032), Msg1032, Hash1032),
    new TestVector(8 * sizeof(Msg1264), Msg1264, Hash1264),
    new TestVector(8 * sizeof(Msg2040), Msg2040, Hash2040),
};

bool Validate0(Keccak_HashInstance& hash)
{
    if (SUCCESS != Keccak_HashInitialize_SHA3_512(&hash))
        return false;

    unsigned char buffer[512 / 8];

    if (hash.fixedOutputLength != 8 * sizeof(buffer))
        return false;

    auto result = Keccak_HashFinal(&hash, buffer);

    if constexpr (sizeof(Hash0) != sizeof(buffer))
        return false;

    return std::equal(std::begin(Hash0), std::end(Hash0), stdext::make_unchecked_array_iterator(buffer));
}

bool Validate(Keccak_HashInstance& hash, const TestVector* testVector)
{
    if (SUCCESS != Keccak_HashInitialize_SHA3_512(&hash))
        return false;

    unsigned char buffer[512 / 8];

    if (hash.fixedOutputLength != 8 * sizeof(buffer))
    {
        printf("Validate %d fixedOutputLength %d\n", testVector->BitLength, hash.fixedOutputLength);
        return false;
    }

    if (SUCCESS != Keccak_HashUpdate(&hash, testVector->Message, testVector->BitLength))
    {
        printf("Validate %d HashUpdate failed\n", testVector->BitLength);

        return false;
    }

    auto result = Keccak_HashFinal(&hash, buffer);

    auto isOk = std::equal(std::begin(buffer), std::end(buffer),
                           stdext::make_unchecked_array_iterator(testVector->Hash));

    if (!isOk)
        printf("Validate %d mismatch\n", testVector->BitLength);

    return isOk;
}

bool UnmanagedValidate()
{
    Keccak_HashInstance hash;

    if (!Validate0(hash))
        return false;

    for (auto tv : TestVectors)
    {
        if (!Validate(hash, tv))
            return false;
    }

    return true;
}

#pragma managed

static Keccak::Keccak()
{
    Validate();
}

void Keccak::Validate()
{
    if (!UnmanagedValidate())
        throw gcnew Security::Cryptography::CryptographicException(L"Validation failed");
}

void Keccak::Initialize()
{
    hash_.Create();

    auto ret = Keccak_HashInitialize_SHA3_512(hash_.Get());

    if (SUCCESS != ret)
        throw gcnew Security::Cryptography::CryptographicException(L"Internal error");

    auto hash = hash_.Get();

    this->HashSizeValue = hash->fixedOutputLength;
}

void Keccak::HashCore(array<unsigned char>^ buffer, int offset, int length)
{
    if (offset < 0 || offset > buffer->Length)
        throw gcnew ArgumentOutOfRangeException(L"offset");

    if (length < 0 || length + offset > buffer->Length)
        throw gcnew ArgumentOutOfRangeException(L"length");

    if (nullptr == hash_.Get())
        Initialize();

    if (length < 1)
        return;

    pin_ptr<unsigned char> p = &buffer[offset];

    auto ret = Keccak_HashUpdate(hash_.Get(), p, length * 8);

    p = nullptr;

    if (SUCCESS != ret)
        throw gcnew Security::Cryptography::CryptographicException(L"Internal error");
}

array<unsigned char>^ Keccak::HashFinal()
{
    auto buffer = gcnew array<unsigned char>(hash_->fixedOutputLength / 8);

    pin_ptr<unsigned char> p = &buffer[0];

    Keccak_HashFinal(hash_.Get(), p);

    p = nullptr;

    return buffer;
}
}
