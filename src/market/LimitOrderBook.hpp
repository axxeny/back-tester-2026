#pragma once

#include "core/Events.hpp"
#include "market/MarketDataEvent.hpp"

#include <cstddef>
#include <functional>
#include <map>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <vector>

namespace cmf::market {

class BookError : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

struct HistoricalBookLevel {
  PriceTicks price{};
  Quantity quantity{};
  Sequence revision{};
};

class LimitOrderBook {
public:
  void apply(const MarketDataEvent &event);

  [[nodiscard]] std::optional<HistoricalBookLevel> best_bid() const;
  [[nodiscard]] std::optional<HistoricalBookLevel> best_ask() const;
  [[nodiscard]] std::vector<HistoricalBookLevel>
  top_bids(std::size_t depth) const;
  [[nodiscard]] std::vector<HistoricalBookLevel>
  top_asks(std::size_t depth) const;
  [[nodiscard]] std::optional<HistoricalBookLevel>
  level(Side side, PriceTicks price) const;
  [[nodiscard]] std::size_t order_count() const noexcept {
    return orders_.size();
  }
  [[nodiscard]] Sequence book_revision() const noexcept {
    return book_revision_;
  }

  [[nodiscard]] std::uint64_t total_adds() const noexcept {
    return total_adds_;
  }
  [[nodiscard]] std::uint64_t total_cancels() const noexcept {
    return total_cancels_;
  }
  [[nodiscard]] std::uint64_t total_modifies() const noexcept {
    return total_modifies_;
  }
  [[nodiscard]] std::uint64_t total_fills() const noexcept {
    return total_fills_;
  }
  [[nodiscard]] std::uint64_t total_trades() const noexcept {
    return total_trades_;
  }
  [[nodiscard]] std::uint64_t total_clears() const noexcept {
    return total_clears_;
  }

private:
  struct Order {
    Side side{Side::None};
    PriceTicks price{};
    Quantity quantity{};
  };

  struct Level {
    Quantity quantity{};
    Sequence revision{};
  };

  using BidLevels = std::map<PriceTicks, Level, std::greater<PriceTicks>>;
  using AskLevels = std::map<PriceTicks, Level>;

  void add(const MarketDataEvent &event);
  void cancel(const MarketDataEvent &event);
  void modify(const MarketDataEvent &event);
  void fill(const MarketDataEvent &event);
  void clear();
  void remove_order(ExchangeOrderId order_id, const Order &order);
  void change_level(Side side, PriceTicks price, Quantity delta);
  [[nodiscard]] Sequence next_revision();

  BidLevels bids_;
  AskLevels asks_;
  std::unordered_map<ExchangeOrderId, Order> orders_;
  Sequence revision_counter_{};
  Sequence book_revision_{};
  std::uint64_t total_adds_{};
  std::uint64_t total_cancels_{};
  std::uint64_t total_modifies_{};
  std::uint64_t total_fills_{};
  std::uint64_t total_trades_{};
  std::uint64_t total_clears_{};
};

using HistoricalLOB = LimitOrderBook;

} // namespace cmf::market
