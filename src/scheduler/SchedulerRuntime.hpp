#pragma once

#include "scheduler/ChronologicalScheduler.hpp"
#include "scheduler/ReadyBarrier.hpp"
#include "scheduler/SpscRing.hpp"

#include <atomic>
#include <concepts>
#include <exception>
#include <limits>
#include <mutex>
#include <optional>
#include <span>
#include <thread>
#include <utility>

namespace cmf::scheduler {

class CommandSink {
public:
  explicit CommandSink(SpscRing<OrderCommand> &ring) : ring_(ring) {}

  bool push(NewOrderCommand command) {
    return ring_.push_wait(OrderCommand{command});
  }

  bool push(CancelCommand command) {
    return ring_.push_wait(OrderCommand{command});
  }

  [[nodiscard]] bool stop_requested() const noexcept { return ring_.closed(); }

private:
  SpscRing<OrderCommand> &ring_;
};

struct SchedulerRuntimeConfig {
  std::size_t event_ring_capacity{1};
  std::size_t command_ring_capacity{64};
  std::size_t maximum_pending_events{4096};
};

class SpanScheduledEventSource {
public:
  explicit SpanScheduledEventSource(std::span<const ScheduledEvent> events)
      : events_(events) {}

  bool next(ScheduledEvent &event) {
    if (index_ == events_.size()) {
      return false;
    }
    event = events_[index_++];
    return true;
  }

private:
  std::span<const ScheduledEvent> events_;
  std::size_t index_{};
};

// A source is pulled one delivery at a time. Any non-owning spans in the
// returned MarketDelivery must remain valid until the next call to next();
// SchedulerRuntime does not request it before processed_seq acknowledges the
// current delivery.
template <typename Source>
concept ScheduledEventSource = requires(Source &source, ScheduledEvent &event) {
  { source.next(event) } -> std::same_as<bool>;
};

class SchedulerRuntime {
public:
  explicit SchedulerRuntime(SchedulerRuntimeConfig config)
      : event_ring_(config.event_ring_capacity),
        command_ring_(config.command_ring_capacity, &dispatcher_wake_),
        ready_(&dispatcher_wake_), scheduler_(config.maximum_pending_events) {}

  SchedulerRuntime(const SchedulerRuntime &) = delete;
  SchedulerRuntime &operator=(const SchedulerRuntime &) = delete;

  template <typename Consumer>
  void run(std::span<const ScheduledEvent> initial_events,
           Consumer &&consumer) {
    SpanScheduledEventSource source(initial_events);
    run_source(source, consumer);
  }

  template <ScheduledEventSource Source, typename Consumer>
  void run(Source &source, Consumer &&consumer) {
    run_source(source, consumer);
  }

  void request_stop() noexcept {
    // Acq_rel elects the first stop requester and publishes its prior writes.
    stopping_.exchange(true, std::memory_order_acq_rel);
    command_ring_.close();
    event_ring_.close();
    ready_.stop();
    dispatcher_wake_.notify_all();
  }

  [[nodiscard]] Sequence processed_sequence() const noexcept {
    return ready_.processed_sequence();
  }

private:
  template <ScheduledEventSource Source, typename Consumer>
  void run_source(Source &source, Consumer &consumer) {
    if (started_.exchange(true, std::memory_order_acq_rel)) {
      throw SchedulerError("scheduler runtime is one-shot");
    }

    std::thread consumer_thread;
    std::thread dispatcher_thread;
    try {
      consumer_thread =
          std::thread([this, &consumer] { run_consumer(consumer); });
      dispatcher_thread =
          std::thread([this, &source] { run_dispatcher(source); });
    } catch (...) {
      request_stop();
      if (dispatcher_thread.joinable()) {
        dispatcher_thread.join();
      }
      if (consumer_thread.joinable()) {
        consumer_thread.join();
      }
      throw;
    }

    dispatcher_thread.join();
    consumer_thread.join();

    if (first_exception_ != nullptr) {
      std::rethrow_exception(first_exception_);
    }
  }

  template <typename Consumer> void run_consumer(Consumer &consumer) noexcept {
    try {
      CommandSink commands(command_ring_);
      ScheduledEvent event{MarketDelivery{}};
      while (event_ring_.pop_wait(event)) {
        if (stopping_.load(std::memory_order_acquire)) {
          return;
        }
        consumer(event, commands);
        if (stopping_.load(std::memory_order_acquire)) {
          return;
        }
        if (!ready_.publish_processed(event.dispatch_sequence())) {
          throw SchedulerError("processed sequence must increase");
        }
      }
    } catch (...) {
      capture_failure(std::current_exception());
      request_stop();
    }
  }

  template <ScheduledEventSource Source>
  void run_dispatcher(Source &source) noexcept {
    try {
      while (!stopping_.load(std::memory_order_acquire)) {
        prefetch_source(source);
        drain_commands();
        if (!source_event_.has_value() && scheduler_.empty()) {
          event_ring_.close();
          command_ring_.close();
          return;
        }

        auto pending = pop_next();
        const auto pending_key = pending.key();
        if (has_last_key_ && pending_key < last_key_) {
          throw SchedulerError("scheduled event would move time backwards");
        }
        if (next_dispatch_sequence_ == std::numeric_limits<Sequence>::max()) {
          throw SchedulerError("dispatch sequence exhausted");
        }
        ++next_dispatch_sequence_;
        auto dispatched =
            with_dispatch_sequence(pending, next_dispatch_sequence_);

        if (!event_ring_.push_wait(std::move(dispatched))) {
          return;
        }
        wait_for_ack_and_drain_commands(next_dispatch_sequence_);
        if (stopping_.load(std::memory_order_acquire)) {
          return;
        }
        last_key_ = pending_key;
        has_last_key_ = true;
      }
    } catch (...) {
      capture_failure(std::current_exception());
      request_stop();
    }
  }

  template <ScheduledEventSource Source> void prefetch_source(Source &source) {
    if (source_exhausted_ || source_event_.has_value()) {
      return;
    }

    ScheduledEvent event{MarketDelivery{}};
    if (!source.next(event)) {
      source_exhausted_ = true;
      return;
    }
    if (event.priority() != EventPriority::MarketData) {
      throw SchedulerError("historical source produced a command event");
    }
    source_event_.emplace(std::move(event));
  }

  [[nodiscard]] ScheduledEvent pop_next() {
    if (source_event_.has_value() &&
        (scheduler_.empty() ||
         source_event_->key() <= scheduler_.peek_next().key())) {
      auto event = std::move(*source_event_);
      source_event_.reset();
      return event;
    }
    return scheduler_.pop_next();
  }

  void wait_for_ack_and_drain_commands(Sequence expected) {
    while (ready_.processed_sequence() < expected) {
      if (stopping_.load(std::memory_order_acquire)) {
        return;
      }

      drain_commands();
      if (ready_.processed_sequence() >= expected) {
        return;
      }

      const auto epoch = dispatcher_wake_.snapshot();
      drain_commands();
      if (ready_.processed_sequence() >= expected ||
          stopping_.load(std::memory_order_acquire)) {
        continue;
      }
      dispatcher_wake_.wait(epoch);
    }
  }

  void drain_commands() {
    OrderCommand command{NewOrderCommand{}};
    while (command_ring_.try_pop(command)) {
      const auto key = std::visit(
          [](const auto &payload) { return ScheduledEvent{payload}.key(); },
          command);
      if (has_last_key_ && key < last_key_) {
        throw SchedulerError("command arrival would move time backwards");
      }
      scheduler_.push(command);
    }
  }

  void capture_failure(std::exception_ptr failure) noexcept {
    std::lock_guard lock(exception_mutex_);
    if (first_exception_ == nullptr) {
      first_exception_ = std::move(failure);
    }
  }

  WakeSignal dispatcher_wake_;
  SpscRing<ScheduledEvent> event_ring_;
  SpscRing<OrderCommand> command_ring_;
  ReadyBarrier ready_;
  ChronologicalScheduler scheduler_;
  std::optional<ScheduledEvent> source_event_;
  bool source_exhausted_{false};
  std::atomic<bool> stopping_{false};
  std::atomic<bool> started_{false};
  Sequence next_dispatch_sequence_{};
  ScheduledKey last_key_{};
  bool has_last_key_{false};
  std::mutex exception_mutex_;
  std::exception_ptr first_exception_;
};

} // namespace cmf::scheduler
