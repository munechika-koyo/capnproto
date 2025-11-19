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

#pragma once

#include "kj/async-prelude.h"
#include "kj/common.h"
#include "kj/debug.h"
#include "kj/exception.h"
#include "kj/source-location.h"
#include <coroutine>
#include <kj/async.h>

namespace kj {

#if !defined(FUT_DEBUG)
#ifdef KJ_DEBUG
#define FUT_DEBUG 1
#else
#define FUT_DEBUG 0
#endif
#endif

#if FUT_DEBUG
#define FUT_DBG(...) KJ_DBG(__VA_ARGS__)
#else
#define FUT_DBG(...)
#endif

template <typename T, bool Lazy> class FutCoroBase;
template <typename T, bool Lazy> class FutCoro;
template <typename T, bool Lazy> class FutPromiseNode;
template <typename T, bool Lazy = false> class Fut;

template <typename T> using LazyFut = Fut<T, true>;

template <typename T, bool Lazy>
using FutHandle = std::coroutine_handle<FutCoroBase<T, Lazy>>;

template <typename T> constexpr bool isVoid = kj::isSameType<T, void>();

// promise-like object
template <typename T, bool Lazy> class Fut {
public:
  using promise_type = FutCoro<T, Lazy>;

  Fut(FutHandle<T, Lazy> h) : handle(h) {
    FUT_DBG("Fut::Fut", this, handle.address());
    handle.promise().fut = this;
  }

  Fut(Fut<T, Lazy> &&other) : handle(kj::mv(other.handle)) {
    FUT_DBG("Fut::Fut&&", this, &other);
    other.handle = {};

    event = other.event;
    other.event = nullptr;

    if (handle) {
      KJ_ASSERT(!handle.done());
      handle.promise().fut = this;
    } else {
      result = kj::mv(other.result);
    }
  }

  Fut(const Fut<T, Lazy> &other) = delete;

  ~Fut() {
    FUT_DBG("Fut::~Fut", this);
    KJ_ASSERT(done(), handle == nullptr);
  }

  bool done() const {
    // we always clear the handle on resolve
    return !handle;
  }

  T wait(WaitScope &waitScope, SourceLocation location = {});

  // Awaiter implementation

  bool await_ready() {
    FUT_DBG("Fut::await_ready", this, done());
    // no need to suspend if we're already done
    return done();
  }

  // called by co_await if await_ready() returned false.
  void await_suspend(std::coroutine_handle<> awaiter) {
    FUT_DBG("Fut::await_suspend", this, awaiter.address(), &promise());
    promise().awaiter = awaiter;
  }

  T await_resume() {
    FUT_DBG("Fut::await_resume", this);
    return _::convertToReturn(kj::mv(result));
  }

private:
  FutHandle<T, Lazy> handle = {};
  _::ExceptionOr<_::FixVoid<T>> result;

  // todo: get rid of this?
  _::OnReadyEvent *event = nullptr;

  FutCoroBase<T, Lazy> &promise() {
    KJ_ASSERT(handle != nullptr);
    return handle.promise();
  }

  void onReady(_::OnReadyEvent *event) noexcept {
    FUT_DBG("Fut::onReady", this, done());
    if (done()) {
      event->arm();
    } else {
      this->event = event;
    }
  }

  void resolveValue(_::FixVoid<T> &&t) {
    FUT_DBG("Fut::resolveValue", this, event);
    result.value = kj::mv(t);
    handle = {};
    if (event != nullptr) {
      event->arm();
      event = nullptr;
    }
  }

  void resolveException(kj::Exception &&e) {
    FUT_DBG("Fut:resolveException", this, handle.address());
    result.exception = kj::mv(e);
    handle = {};
    if (event != nullptr) {
      event->arm();
      event = nullptr;
    }
  }

  void get(_::ExceptionOr<_::FixVoid<T>> &output) noexcept {
    KJ_ASSERT(done());
    output = kj::mv(result);
  }

  friend class FutCoroBase<T, Lazy>;
  friend class FutCoro<T, Lazy>;
  friend class FutPromiseNode<T, Lazy>;
};

// Awaits for kj::Promise from within a Fut-coroutine
template <typename T> class PromiseFutAwaiter : public _::Event {
public:
  PromiseFutAwaiter(_::OwnPromiseNode &&promise, SourceLocation location)
      : _::Event(location), node(kj::mv(promise)) {
    FUT_DBG("PromiseFutAwaiter::PromiseFutAwaiter", this, node);
  }

  virtual ~PromiseFutAwaiter() {
    FUT_DBG("PromiseFutAwaiter::~PromiseFutAwaiter", this, node);
  }

  bool await_ready() {
    FUT_DBG("PromiseFutAwaiter::await_ready => false", this, node);
    // promises always suspend initially
    return false;
  }

  bool await_suspend(std::coroutine_handle<> h) {
    FUT_DBG("PromiseFutAwaiter::await_suspend", this, node, h.done(),
            h.address());
    this->handle = h;
    node->setSelfPointer(&node);
    node->onReady(this);

    if (isNext()) {
      disarm();
      FUT_DBG("PromiseFutAwaiter::await_suspend => false", this, node);
      return false;
    }

    FUT_DBG("PromiseFutAwaiter::await_suspend => true", this, node);
    return true;
  }

  T await_resume() {
    FUT_DBG("PromiseFutAwaiter::await_resume", this, node);
    _::ExceptionOr<_::FixVoid<T>> result;
    node->get(result);
    return _::convertToReturn(kj::mv(result));
  }

  void traceEvent(_::TraceBuilder &builder) override {
    FUT_DBG("PromiseFutAwaiter::traceEvent", this);
    KJ_UNIMPLEMENTED("TODO");
  }

  Maybe<Own<Event>> fire() override {
    FUT_DBG("PromiseFutAwaiter::fire", this, node);
    auto h = handle;
    handle = {};
    h.resume();
    return kj::none;
  }

  _::OwnPromiseNode node;
  std::coroutine_handle<> handle = {};
};

struct LazyInitialSuspend {
  LazyInitialSuspend() : promise(kj::READY_NOW) {
    FUT_DBG("LazyInitialSuspend::LazyInitialSuspend", this);
  }
  ~LazyInitialSuspend() {
    FUT_DBG("LazyInitialSuspend::~LazyInitialSuspend", this);
  }
  bool await_ready() const noexcept {
    FUT_DBG("LazyInitialSuspend::await_ready", this);
    return false;
  }

  auto await_suspend(std::coroutine_handle<> h) {
    // todo(perf): maybe this is not the most efficient way to resume on the
    // next event loop iteration?
    promise = kj::evalLater([h = kj::mv(h)]() {
                FUT_DBG("LazyInitialSuspend::await_suspend::evalLater");
                h.resume();
              }).eagerlyEvaluate(nullptr);
    FUT_DBG("LazyInitialSuspend::await_suspend", this, &promise);
  }

  void await_resume() const noexcept {
    FUT_DBG("LazyInitialSuspend::await_resume", this);
  }

  kj::Promise<void> promise;
};

kj::byte *futAlloc(size_t size);
void futFree(kj::byte *ptr);

// Coroutine promise_type. We call it FutCoro not to be confused with promises
// and also because its lifetime directly corresponds to a coro frame.
template <typename T, bool Lazy> class FutCoroBase {
public:
  FutCoroBase() { FUT_DBG("FutCoro::FutCoro", this, Lazy); }

  ~FutCoroBase() {
    FUT_DBG("FutCoro::~FutCoro", this, fut);
    KJ_ASSERT(fut == nullptr);
  }

  auto initial_suspend() noexcept {
    FUT_DBG("FutCoro::initial_suspend", this, Lazy);
    if constexpr (Lazy) {
      return LazyInitialSuspend{};
    } else {
      return std::suspend_never{};
    }
  }

  auto final_suspend() noexcept {
    FUT_DBG("FutCoro::final_suspend", this, Lazy, awaiter == nullptr);
    if (awaiter) {
      awaiter.resume();
    }
    return std::suspend_never{};
  }

  Fut<T, Lazy> get_return_object() {
    FUT_DBG("FutCoro::get_return_object", this);
    return Fut<T, Lazy>(FutHandle<T, Lazy>::from_promise(*this));
  }

  void unhandled_exception() {
    fut->resolveException(kj::getCaughtExceptionAsKj());
    this->fut = nullptr;
  }

  // await_transform

  template <typename U, bool UEnabled>
  Fut<U, UEnabled> await_transform(Fut<U, UEnabled> &&fut) {
    // child coro creation goes through this
    FUT_DBG("FutCoro::await_transform Fut", this, &fut);
    return kj::mv(fut);
  }

  template <typename U>
  PromiseFutAwaiter<U> await_transform(kj::Promise<U> &&promise) {
    // child coro creation goes through this
    FUT_DBG("FutCoro::await_transform Promise", this, &promise);
    return PromiseFutAwaiter<U>(_::PromiseNode::from(kj::mv(promise)), {});
  }

  // coroutine frame allocation

  template <typename... Args>
  inline void *operator new(size_t size, Args &&...args) {
    auto ptr = futAlloc(size);
    FUT_DBG("new", size, ptr);
    return ptr;
  }

  inline void operator delete(void *ptr) {
    FUT_DBG("delete", ptr);
    futFree(static_cast<kj::byte *>(ptr));
  }

protected:
  // Will update fut result on completion.
  // Set and maintain by Fut constructors.
  // Becomes nullptr after completion again.
  Fut<T, Lazy> *fut = nullptr;

private:
  std::coroutine_handle<> awaiter = {};

  friend class Fut<T, Lazy>;
};

template <typename T, bool Lazy> class FutCoro : public FutCoroBase<T, Lazy> {
public:
  void return_value(T &&value) {
    FUT_DBG("FutCoro::return_value", this);
    this->fut->resolveValue(kj::mv(value));
    this->fut = nullptr;
  }
};

template <bool Lazy>
class FutCoro<void, Lazy> : public FutCoroBase<void, Lazy> {
public:
  void return_void() {
    FUT_DBG("FutCoro::return_void", this);
    this->fut->resolveValue({});
    this->fut = nullptr;
  }
};

//---------------------
// KJ event loop integration

template <typename T, bool Lazy> class FutPromiseNode : public _::PromiseNode {
public:
  FutPromiseNode(Fut<T, Lazy> &fut) : fut(fut) {
    FUT_DBG("FutPromiseNode::FutPromiseNode", this, &fut);
  }

  virtual ~FutPromiseNode() {
    FUT_DBG("FutPromiseNode::~FutPromiseNode", this, &fut);
  }

  void destroy() override { FUT_DBG("FutPromiseNode::destroy", this, &fut); }

  void onReady(_::Event *event) noexcept override {
    FUT_DBG("FutPromiseNode::onReady", this, &fut);
    onReadyEvent.init(event);
    fut.onReady(&onReadyEvent);
  }

  void get(_::ExceptionOrValue &output) noexcept override {
    FUT_DBG("FutPromiseNode::get", this, &fut, &output);
    auto &result = output.as<_::FixVoid<T>>();
    fut.get(result);
  }

  void tracePromise(_::TraceBuilder &builder, bool stopAtNextEvent) override {
    KJ_UNIMPLEMENTED("todo");
  }

private:
  Fut<T, Lazy> &fut;
  _::OnReadyEvent onReadyEvent;
};

template <typename T, bool Lazy>
inline T Fut<T, Lazy>::wait(WaitScope &waitScope, SourceLocation location) {
  FUT_DBG("Fut::wait", this);
  _::ExceptionOr<_::FixVoid<T>> result;
  auto node = _::PromiseDisposer::alloc<FutPromiseNode<T, Lazy>>(*this);
  _::waitImpl(kj::mv(node), result, waitScope, location);
  return _::convertToReturn(kj::mv(result));
}
} // namespace kj
