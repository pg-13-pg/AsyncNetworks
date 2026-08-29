#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>

constexpr size_t MEMORY_POOL_NUM = 64;
constexpr size_t SLOT_BASE_SIZE = 8;
constexpr size_t MAX_SLOT_SIZE = 512;

struct Slot
{
    Slot *next;
};

// 一个内存池管理多个内存块，每个内存块的内存槽大小一致  MemoryPool->Block->Slot
class MemoryPool
{
  public:
    // BlockSize默认设计为4096字节是因为作系统页大小通常是 4KB，与页对齐可以提高内存访问效率，避免跨页访问导致的性能损失
    MemoryPool(size_t blockSize = 4096);
    ~MemoryPool();

    void init(size_t slotSize); // 延迟初始化
    void *allocate();           // 分配一个内存槽，返回槽指针
    void deallocate(void *p);   // 回收内存槽到空闲槽链表
  private:
    void allocateNewBlock();                     // 向OS申请一个新内存块
    size_t padPointer(char *p, size_t slotSize); // 计算内存对齐

    size_t blockSize_; // 内存块大小
    size_t slotSize_;  // 槽大小
    Slot *firstBlock_; // 指向内存池管理的首个内存块
    Slot *curSlot_;    // 指向当前内存块中下一个未被使用过的槽
    Slot *freeList_;   // 空闲槽链表头指针，管理已分配后释放的复用槽位
    Slot *lastSlot_;   // 当前内存块中最后能够存放元素的内存槽指针(超过该位置需申请新的内存块)

    std::mutex mutexForFreeList_; // 保证freeList_在多线程中操作的原子性
    std::mutex mutexForBlock_;    // 保证内存块管理在多线程中操作的原子性
};

class HashBucket
{
  public:
    static void initMemoryPool();
    // 单例模式
    static MemoryPool &getMemoryPool(int index); // 获取内存池接口
    static void *useMemory(size_t size);
    static void freeMemory(void *p, size_t size);

    template <typename T, typename... Args>
    friend T *newElement(Args &&...args); // 提供给用户在内存池分配的内存中创建对象的外部接口

    template <typename T> friend void deleteElement(T *p); // 析构内存池分配的内存中的对象

  private:
    static MemoryPool memoryPool[MEMORY_POOL_NUM]; // 设置为static，借助 C++11 的线程安全静态初始化保证只初始化一次
};

// T为对象类型，Args为T的构造函数参数类型 ，res为内存池中的内存槽指针
template <typename T, typename... Args> inline T *newElement(Args &&...args)
{
    T *res = reinterpret_cast<T *>(HashBucket::useMemory(sizeof(T))); // 从内存池中申请原始内存
    if (res)
    {
        // placement new与完美转发，在res指向的内存中调用T的构造函数，传入参数args
        new (res) T(std::forward<Args>(args)...);
    }
    return res;
}

// 析构对象，传入对象的指针，调用析构函数并释放内存池中的内存槽
template <typename T> inline void deleteElement(T *p)
{
    if (p)
    {
        p->~T();                                                        // 调用对象的析构函数，释放对象占用的资源
        HashBucket::freeMemory(reinterpret_cast<void *>(p), sizeof(T)); // 将对象占用的内存槽归还给内存池
    }
}
