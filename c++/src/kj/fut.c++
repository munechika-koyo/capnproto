// Copyright (c) 2025 Cloudflare, Inc. and contributors
// Licensed under the MIT License:
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#include "fut.h"

namespace kj {

#define FUT_USE_ALLOCATOR 1

struct FutAllocator {
  struct Chunk {
    size_t offset = 0;
    kj::byte bytes[16 * 1024];

    kj::byte *begin() { return bytes; }
    kj::byte *end() { return bytes + size(); }

    size_t size() { return sizeof(bytes); };

    kj::byte *alloc(size_t n) {
#if FUT_USE_ALLOCATOR      
      KJ_ASSERT(offset + n < size(), "not implemented");
      auto ptr = bytes + offset;
      offset += n;
      // ASAN_UNPOISON_MEMORY_REGION(ptr, size);
      return ptr;
#else 
      return new kj::byte[n];
#endif
    }

    void free(kj::byte *ptr, size_t n) {
#if FUT_USE_ALLOCATOR      
      KJ_ASSERT(ptr + n == bytes + offset);
      offset -= n;
      // ASAN_POISON_MEMORY_REGION(ptr, size);
    }
#else
      delete[] ptr;
#endif
  };

  struct Frame {
    size_t dataSize;
    kj::byte data[];

    kj::byte *dataBegin() { return data; }
    kj::byte *dataEnd() { return data + dataSize; }

    size_t allocSize() { return sizeof(Frame) + dataSize; }
    static size_t allocSize(size_t dataSize) {
      return sizeof(Frame) + dataSize;
    }
  };

  static_assert(alignof(Frame) == alignof(size_t),
                "size_t alignment is expected");

  kj::byte *alloc(size_t frameSize) {
    auto allocSize = Frame::allocSize(frameSize);
    // auto ptr = ::operator new(allocSize);
    auto ptr = chunk.alloc(allocSize);
    auto frame = reinterpret_cast<Frame *>(ptr);
    frame->dataSize = frameSize;
    return frame->dataBegin();
  }

  void free(kj::byte *ptr) {
    auto frame = reinterpret_cast<Frame *>((kj::byte *)ptr - sizeof(Frame));
    // ::operator delete[] (reinterpret_cast<kj::byte*>(frame),
    // frame->allocSize());
    chunk.free(reinterpret_cast<kj::byte *>(frame), frame->allocSize());
  }

  Chunk chunk;
};

static thread_local FutAllocator localAllocator;

kj::byte *futAlloc(size_t size) { return localAllocator.alloc(size); }

void futFree(kj::byte *ptr) { localAllocator.free(ptr); }

} // namespace kj