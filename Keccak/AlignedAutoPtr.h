// Based on http://msdn.microsoft.com/en-us/library/aa730837(v=vs.80).aspx
// Note that the article was written for VS2005.
template <typename T>
ref struct AlignedAutoPtr
{
    AlignedAutoPtr() : m_ptr(nullptr) {}
    AlignedAutoPtr(AlignedAutoPtr<T>% right) : m_ptr(right.Release()) {}

    ~AlignedAutoPtr()
    {
        Cleanup();
    }

    !AlignedAutoPtr()
    {
        _ASSERT(nullptr == m_ptr);
        Cleanup();
    }

    T* operator->()
    {
        _ASSERT(nullptr != m_ptr);
        return m_ptr;
    }

    T* Get()
    {
        return m_ptr;
    }

    //T* Release()
    //{
    //    T* released = m_ptr;
    //    m_ptr = nullptr;
    //    return released;
    //}

    void Reset()
    {
        Reset(nullptr);
    }

    void Create()
    {
        if (nullptr != m_ptr)
            return;

        auto p = reinterpret_cast<T *>(_aligned_malloc(sizeof(T), __alignof(T)));

        m_ptr = p;
    }
private:
    void Reset(T* ptr)
    {
        if (ptr != m_ptr)
        {
            Cleanup();
            m_ptr = ptr;
        }
    }

    T* m_ptr;

    void Cleanup()
    {
        if (nullptr == m_ptr)
            return;

        m_ptr->~T();

        _aligned_free(m_ptr);

        m_ptr = nullptr;
    }
};