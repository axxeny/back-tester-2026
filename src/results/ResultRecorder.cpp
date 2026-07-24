#include "results/ResultRecorder.hpp"

#include <algorithm>
#include <limits>
#include <map>
#include <type_traits>
#include <utility>
#include <vector>

namespace cmf::results {
namespace {

struct Rational {
  std::int64_t numerator{};
  std::int64_t denominator{1};
};

[[nodiscard]] unsigned __int128 magnitude(__int128 value) noexcept {
  if (value >= 0) {
    return static_cast<unsigned __int128>(value);
  }
  return static_cast<unsigned __int128>(-(value + 1)) + 1;
}

[[nodiscard]] unsigned __int128 gcd128(unsigned __int128 left,
                                       unsigned __int128 right) noexcept {
  while (right != 0) {
    const auto remainder = left % right;
    left = right;
    right = remainder;
  }
  return left;
}

[[nodiscard]] Rational normalize(__int128 numerator, __int128 denominator) {
  if (denominator <= 0) {
    throw ResultError("PnL denominator must be positive");
  }
  if (numerator == 0) {
    return {};
  }
  const auto divisor = gcd128(magnitude(numerator), magnitude(denominator));
  numerator /= static_cast<__int128>(divisor);
  denominator /= static_cast<__int128>(divisor);
  if (numerator < std::numeric_limits<std::int64_t>::min() ||
      numerator > std::numeric_limits<std::int64_t>::max() ||
      denominator > std::numeric_limits<std::int64_t>::max()) {
    throw ResultError("exact PnL amount exceeds native rational range");
  }
  return {static_cast<std::int64_t>(numerator),
          static_cast<std::int64_t>(denominator)};
}

[[nodiscard]] __int128 multiply128(__int128 left, __int128 right) {
  __int128 result{};
  if (__builtin_mul_overflow(left, right, &result)) {
    throw ResultError("PnL multiplication overflow");
  }
  return result;
}

[[nodiscard]] __int128 add128(__int128 left, __int128 right) {
  __int128 result{};
  if (__builtin_add_overflow(left, right, &result)) {
    throw ResultError("PnL addition overflow");
  }
  return result;
}

[[nodiscard]] Rational add(Rational left, Rational right) {
  const __int128 left_scaled = multiply128(left.numerator, right.denominator);
  const __int128 right_scaled = multiply128(right.numerator, left.denominator);
  const __int128 numerator = add128(left_scaled, right_scaled);
  const __int128 denominator = multiply128(left.denominator, right.denominator);
  return normalize(numerator, denominator);
}

[[nodiscard]] Rational scaled_price_delta(__int128 price_delta_numerator,
                                          __int128 price_delta_denominator,
                                          Quantity signed_quantity,
                                          Quantity multiplier,
                                          PriceTicks price_scale) {
  __int128 numerator = multiply128(price_delta_numerator, signed_quantity);
  numerator = multiply128(numerator, multiplier);
  const __int128 denominator =
      multiply128(price_delta_denominator, price_scale);
  return normalize(numerator, denominator);
}

template <typename Vector> void reserve_one_more(Vector &values) {
  if (values.size() < values.capacity()) {
    return;
  }
  const std::size_t maximum = values.max_size();
  if (values.size() == maximum) {
    throw ResultError("result column capacity exhausted");
  }
  const std::size_t doubled =
      values.capacity() > maximum / 2 ? maximum : values.capacity() * 2;
  const std::size_t target =
      std::max(values.size() + 1, std::max<std::size_t>(1, doubled));
  values.reserve(target);
}

template <typename... Vectors> void reserve_row(Vectors &...vectors) {
  (reserve_one_more(vectors), ...);
}

[[nodiscard]] bool valid_side(Side side) noexcept {
  return side == Side::Buy || side == Side::Sell;
}

} // namespace

class FrozenResults::Storage {
public:
  struct FillColumns {
    std::vector<TimestampNs> exchange_ts_ns;
    std::vector<TimestampNs> engine_ts_ns;
    std::vector<InstrumentId> instrument_id;
    std::vector<ClOrdId> client_order_id;
    std::vector<Side> side;
    std::vector<PriceTicks> price_ticks;
    std::vector<Quantity> quantity;
    std::vector<Quantity> remaining_quantity;
    std::vector<LiquiditySource> liquidity_source;
  } fills;

  struct OrderColumns {
    std::vector<TimestampNs> engine_ts_ns;
    std::vector<InstrumentId> instrument_id;
    std::vector<ClOrdId> client_order_id;
    std::vector<OrderLogEventType> event_type;
    std::vector<OrderState> state;
    std::vector<Side> side;
    std::vector<PriceTicks> limit_price_ticks;
    std::vector<Quantity> order_quantity;
    std::vector<Quantity> filled_quantity;
    std::vector<Quantity> remaining_quantity;
    std::vector<RejectReason> reject_reason;
  } orders;

  struct PnlColumns {
    std::vector<TimestampNs> engine_ts_ns;
    std::vector<AccountCurrencyAmount> exact_total;
    std::vector<double> total_pnl;
  } pnl;

  bool frozen{};
};

class ResultRecorder::Impl {
public:
  struct Lot {
    Side side{Side::None};
    PriceTicks price{};
    Quantity quantity{};
  };

  struct Ledger {
    InstrumentMeta meta;
    Quantity net_quantity{};
    Rational realized;
    std::vector<Lot> lots;
    bool has_mark{};
    __int128 mark_numerator{};
    std::int64_t mark_denominator{1};
  };

  Impl(std::span<const InstrumentMeta> instruments,
       ResultReserveEstimate estimate)
      : storage(std::make_shared<FrozenResults::Storage>()) {
    for (const auto &meta : instruments) {
      if (meta.instrument_id <= 0 || meta.tick_size_ticks <= 0 ||
          meta.price_scale <= 0 || meta.contract_multiplier <= 0) {
        throw std::invalid_argument(
            "instrument metadata values must be positive");
      }
      const auto [iterator, inserted] = ledgers.emplace(
          meta.instrument_id, Ledger{meta, 0, {}, {}, false, 0, 1});
      (void)iterator;
      if (!inserted) {
        throw std::invalid_argument("duplicate instrument metadata");
      }
    }
    reserve_fills(estimate.fills);
    reserve_orders(estimate.order_events);
    storage->pnl.engine_ts_ns.reserve(estimate.pnl_points);
    storage->pnl.exact_total.reserve(estimate.pnl_points);
  }

  void ensure_mutable() const {
    if (storage->frozen) {
      throw ResultError("result buffers are frozen");
    }
  }

  void reserve_fills(std::size_t count) {
    auto &c = storage->fills;
    c.exchange_ts_ns.reserve(count);
    c.engine_ts_ns.reserve(count);
    c.instrument_id.reserve(count);
    c.client_order_id.reserve(count);
    c.side.reserve(count);
    c.price_ticks.reserve(count);
    c.quantity.reserve(count);
    c.remaining_quantity.reserve(count);
    c.liquidity_source.reserve(count);
  }

  void reserve_orders(std::size_t count) {
    auto &c = storage->orders;
    c.engine_ts_ns.reserve(count);
    c.instrument_id.reserve(count);
    c.client_order_id.reserve(count);
    c.event_type.reserve(count);
    c.state.reserve(count);
    c.side.reserve(count);
    c.limit_price_ticks.reserve(count);
    c.order_quantity.reserve(count);
    c.filled_quantity.reserve(count);
    c.remaining_quantity.reserve(count);
    c.reject_reason.reserve(count);
  }

  [[nodiscard]] Rational instrument_total(const Ledger &ledger) const {
    Rational total = ledger.realized;
    if (!ledger.has_mark) {
      return total;
    }
    for (const auto &lot : ledger.lots) {
      const __int128 lot_at_mark_denominator =
          multiply128(lot.price, ledger.mark_denominator);
      const __int128 difference =
          add128(ledger.mark_numerator, -lot_at_mark_denominator);
      const Quantity signed_quantity =
          lot.side == Side::Buy ? lot.quantity : -lot.quantity;
      total = add(total, scaled_price_delta(difference, ledger.mark_denominator,
                                            signed_quantity,
                                            ledger.meta.contract_multiplier,
                                            ledger.meta.price_scale));
    }
    return total;
  }

  [[nodiscard]] Rational aggregate_total(InstrumentId override_id,
                                         const Ledger *override_ledger) const {
    Rational aggregate;
    for (const auto &[instrument_id, ledger] : ledgers) {
      const Ledger &selected =
          instrument_id == override_id ? *override_ledger : ledger;
      aggregate = add(aggregate, instrument_total(selected));
    }
    return aggregate;
  }

  void prepare_pnl_append(TimestampNs engine_ts_ns) {
    auto &pnl = storage->pnl;
    if (!pnl.engine_ts_ns.empty() && engine_ts_ns < pnl.engine_ts_ns.back()) {
      throw ResultError("PnL sample time moved backwards");
    }
    if (pnl.engine_ts_ns.empty() || engine_ts_ns != pnl.engine_ts_ns.back()) {
      reserve_row(pnl.engine_ts_ns, pnl.exact_total);
    }
  }

  void commit_pnl(TimestampNs engine_ts_ns, Rational amount) noexcept {
    auto &pnl = storage->pnl;
    const AccountCurrencyAmount public_amount{amount.numerator,
                                              amount.denominator};
    if (!pnl.engine_ts_ns.empty() && engine_ts_ns == pnl.engine_ts_ns.back()) {
      pnl.exact_total.back() = public_amount;
      return;
    }
    pnl.engine_ts_ns.push_back(engine_ts_ns);
    pnl.exact_total.push_back(public_amount);
  }

  std::shared_ptr<FrozenResults::Storage> storage;
  std::map<InstrumentId, Ledger> ledgers;
};

FillColumnsView FrozenResults::fills() const noexcept {
  if (!storage_) {
    return {};
  }
  const auto &c = storage_->fills;
  return {c.exchange_ts_ns,
          c.engine_ts_ns,
          c.instrument_id,
          c.client_order_id,
          c.side,
          c.price_ticks,
          c.quantity,
          c.remaining_quantity,
          c.liquidity_source};
}

OrderLogColumnsView FrozenResults::order_log() const noexcept {
  if (!storage_) {
    return {};
  }
  const auto &c = storage_->orders;
  return {c.engine_ts_ns,       c.instrument_id,  c.client_order_id,
          c.event_type,         c.state,          c.side,
          c.limit_price_ticks,  c.order_quantity, c.filled_quantity,
          c.remaining_quantity, c.reject_reason};
}

PnlColumnsView FrozenResults::pnl() const noexcept {
  if (!storage_) {
    return {};
  }
  return {storage_->pnl.engine_ts_ns, storage_->pnl.total_pnl};
}

std::span<const AccountCurrencyAmount>
FrozenResults::exact_pnl() const noexcept {
  return storage_
             ? std::span<const AccountCurrencyAmount>(storage_->pnl.exact_total)
             : std::span<const AccountCurrencyAmount>{};
}

ResultRecorder::ResultRecorder(std::span<const InstrumentMeta> instruments,
                               ResultReserveEstimate estimate)
    : impl_(std::make_unique<Impl>(instruments, estimate)) {}

ResultRecorder::~ResultRecorder() = default;

void ResultRecorder::on_order_event(const OrderLogResultRow &row) {
  impl_->ensure_mutable();
  auto &c = impl_->storage->orders;
  reserve_row(c.engine_ts_ns, c.instrument_id, c.client_order_id, c.event_type,
              c.state, c.side, c.limit_price_ticks, c.order_quantity,
              c.filled_quantity, c.remaining_quantity, c.reject_reason);
  c.engine_ts_ns.push_back(row.engine_ts_ns);
  c.instrument_id.push_back(row.instrument_id);
  c.client_order_id.push_back(row.client_order_id);
  c.event_type.push_back(row.event_type);
  c.state.push_back(row.state);
  c.side.push_back(row.side);
  c.limit_price_ticks.push_back(row.limit_price_ticks);
  c.order_quantity.push_back(row.order_quantity);
  c.filled_quantity.push_back(row.filled_quantity);
  c.remaining_quantity.push_back(row.remaining_quantity);
  c.reject_reason.push_back(row.reject_reason);
}

void ResultRecorder::on_fill(const FillResultRow &row) {
  static_assert(std::is_nothrow_move_assignable_v<Impl::Ledger>);
  impl_->ensure_mutable();
  const auto found = impl_->ledgers.find(row.instrument_id);
  if (found == impl_->ledgers.end()) {
    throw std::invalid_argument("fill references unknown instrument");
  }
  if (!valid_side(row.side) || row.price_ticks <= 0 || row.quantity <= 0 ||
      row.remaining_quantity < 0) {
    throw std::invalid_argument("invalid fill result row");
  }

  Impl::Ledger staged = found->second;
  const Quantity signed_fill =
      row.side == Side::Buy ? row.quantity : -row.quantity;
  Quantity new_net{};
  if (__builtin_add_overflow(staged.net_quantity, signed_fill, &new_net)) {
    throw ResultError("position quantity overflow");
  }

  Quantity remaining = row.quantity;
  std::size_t consumed_lots = 0;
  while (remaining > 0 && consumed_lots < staged.lots.size() &&
         staged.lots[consumed_lots].side != row.side) {
    auto &lot = staged.lots[consumed_lots];
    const Quantity closing = std::min(remaining, lot.quantity);
    const __int128 price_difference =
        lot.side == Side::Buy
            ? static_cast<__int128>(row.price_ticks) - lot.price
            : static_cast<__int128>(lot.price) - row.price_ticks;
    staged.realized =
        add(staged.realized, scaled_price_delta(price_difference, 1, closing,
                                                staged.meta.contract_multiplier,
                                                staged.meta.price_scale));
    lot.quantity -= closing;
    remaining -= closing;
    if (lot.quantity == 0) {
      ++consumed_lots;
    }
  }
  if (consumed_lots != 0) {
    staged.lots.erase(staged.lots.begin(),
                      staged.lots.begin() +
                          static_cast<std::ptrdiff_t>(consumed_lots));
  }
  if (remaining > 0) {
    staged.lots.push_back(Impl::Lot{row.side, row.price_ticks, remaining});
  }
  staged.net_quantity = new_net;

  const Rational aggregate = impl_->aggregate_total(row.instrument_id, &staged);
  auto &c = impl_->storage->fills;
  reserve_row(c.exchange_ts_ns, c.engine_ts_ns, c.instrument_id,
              c.client_order_id, c.side, c.price_ticks, c.quantity,
              c.remaining_quantity, c.liquidity_source);
  impl_->prepare_pnl_append(row.engine_ts_ns);

  found->second = std::move(staged);
  c.exchange_ts_ns.push_back(row.exchange_ts_ns);
  c.engine_ts_ns.push_back(row.engine_ts_ns);
  c.instrument_id.push_back(row.instrument_id);
  c.client_order_id.push_back(row.client_order_id);
  c.side.push_back(row.side);
  c.price_ticks.push_back(row.price_ticks);
  c.quantity.push_back(row.quantity);
  c.remaining_quantity.push_back(row.remaining_quantity);
  c.liquidity_source.push_back(row.liquidity_source);
  impl_->commit_pnl(row.engine_ts_ns, aggregate);
}

bool ResultRecorder::on_book_mark(InstrumentId instrument_id,
                                  TimestampNs engine_ts_ns,
                                  std::optional<PriceTicks> best_bid,
                                  std::optional<PriceTicks> best_ask) {
  impl_->ensure_mutable();
  const auto found = impl_->ledgers.find(instrument_id);
  if (found == impl_->ledgers.end()) {
    throw std::invalid_argument("mark references unknown instrument");
  }
  if (!best_bid.has_value() || !best_ask.has_value()) {
    return false;
  }
  if (*best_bid <= 0 || *best_ask <= 0 || *best_bid > *best_ask) {
    throw std::invalid_argument("invalid two-sided book mark");
  }

  Impl::Ledger staged = found->second;
  const __int128 midpoint_numerator =
      static_cast<__int128>(*best_bid) + *best_ask;
  if (staged.has_mark && staged.mark_denominator == 2 &&
      staged.mark_numerator == midpoint_numerator) {
    return false;
  }
  staged.has_mark = true;
  staged.mark_numerator = midpoint_numerator;
  staged.mark_denominator = 2;

  if (staged.net_quantity == 0) {
    found->second = std::move(staged);
    return true;
  }
  const Rational aggregate = impl_->aggregate_total(instrument_id, &staged);
  impl_->prepare_pnl_append(engine_ts_ns);
  found->second = std::move(staged);
  impl_->commit_pnl(engine_ts_ns, aggregate);
  return true;
}

Quantity ResultRecorder::position(InstrumentId instrument_id) const {
  const auto found = impl_->ledgers.find(instrument_id);
  return found == impl_->ledgers.end() ? 0 : found->second.net_quantity;
}

FrozenResults ResultRecorder::freeze() {
  impl_->ensure_mutable();
  std::vector<double> converted;
  converted.reserve(impl_->storage->pnl.exact_total.size());
  for (const auto amount : impl_->storage->pnl.exact_total) {
    converted.push_back(static_cast<double>(amount.numerator) /
                        static_cast<double>(amount.denominator));
  }
  impl_->storage->pnl.total_pnl = std::move(converted);
  impl_->storage->frozen = true;
  return FrozenResults{impl_->storage};
}

bool ResultRecorder::frozen() const noexcept { return impl_->storage->frozen; }

} // namespace cmf::results
