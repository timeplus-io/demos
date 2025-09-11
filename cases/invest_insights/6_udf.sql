-- part_rate
-- Continuous Auction Participation Rate
-- Refers to the arithmetic average of the ratio of daily effective participation time in continuous auction to total continuous auction time during the rating period. The calculation formula is:
-- Continuous Auction Participation Rate = avg(Number of Valid Sampling Points / (Market Time Constant - Trading Suspension and Rating Exemption Time))
-- The indicator sampling time is during the continuous auction phase, with sampling conducted every second. No sampling is performed during trading suspensions and bilateral quote exemption periods.
-- Valid sampling point quotes must simultaneously meet the minimum declaration amount and maximum bid-ask spread requirements for the corresponding fund category. That is, valid sampling points must have both the total declared amount of existing buy orders and the total declared amount of existing sell orders not below the minimum declaration amount, and the minimum quote spread not exceeding the maximum bid-ask spread.
-- Minimum Quote Spread: Calculated as follows: Minimum Quote Spread = (Highest Ask Price - Lowest Bid Price) / [max(Highest Ask Price + Lowest Bid Price, 2) / 2]
-- Highest Ask Price and Lowest Bid Price are determined as follows:
-- Existing sell orders in the order book are accumulated from low to high by declared price until the cumulative declared amount meets the minimum declaration amount; the price of that sell order becomes the Highest Ask Price.
-- Existing buy orders in the order book are accumulated from high to low by declared price until the cumulative declared amount meets the minimum declaration amount; the price of that buy order becomes the Lowest Bid Price.

CREATE OR REPLACE AGGREGATE FUNCTION part_rate(
    order_id string, market string, side string, qty float64, cum_qty float64,
    price float64, status string, min_balance float64, min_spread float64)
RETURNS float64
LANGUAGE JAVASCRIPT AS $$
{
  initialize: function() {
      this.sell_orders = new Map();
      this.buy_orders = new Map();
      this.market = 'US';
      this.min_balance = Number.MAX_VALUE;
      this.min_spread = Number.MAX_VALUE;
  },

  isValid: function(old_order, new_order){
    if ('7' === old_order.status && ('4' === new_order.status || '5' === new_order.status)) {

    } else {
      if ('5' === old_order.status && '7' === new_order.status)
        return false;

      if (old_order.status > new_order.status)
        return false;
    }

    if (old_order.cum_qty > new_order.cum_qty)
      return false;

    return true;
  },

  rate: function() {
    if (this.T === undefined)
      this.T = 0.0;

    if (this.market === 'US')
      return this.T / (4*60*60);
    else
      return 0.0;

  },

  process: function (ids, markets, sides, qty, cum_qty, prices, status, min_balance, min_spread) {
      this.market = markets[0];
      this.min_balance = min_balance[0];
      this.min_spread = min_spread[0];
      for (let i = 0; i < sides.length; i++) {
          let order = {
                  price: prices[i],
                  qty: qty[i],
                  cum_qty: cum_qty[i],
                  status: status[i]
          };

          if (this.buy_orders.has(ids[i]))
          {
            if (this.isValid(this.buy_orders.get(ids[i]), order))
              this.buy_orders.set(ids[i], order);
          }
          else if (this.sell_orders.has(ids[i]))
          {
            if (this.isValid(this.sell_orders.get(ids[i]), order))
              this.sell_orders.set(ids[i], order);
          }
          else
          {
            if (sides[i] === '1')
              this.buy_orders.set(ids[i], order);
            else if (sides[i] === '2')
              this.sell_orders.set(ids[i], order);
          }
      }
  },

  finalize: function () {
      if (this.T === undefined)
        this.T = 0.0;

      let max_sell_price = Number.MIN_VALUE;
      let min_buy_price = Number.MAX_VALUE;
      // sort buy_orders
      let sum = 0.0;
      let sorted_buy = [...this.buy_orders.values()];
      sorted_buy.sort(function(a, b){ if (a.price < b.price) return 1; else return -1;});
      for(let i=0; i < sorted_buy.length; i++)
      {
        sum += sorted_buy[i].price * (sorted_buy[i].qty - sorted_buy[i].cum_qty);
        if (sum > this.min_balance)
          min_buy_price = sorted_buy[i].price;
      }

      // sort sell_orders
      sum = 0.0;
      let sorted_sell = [...this.sell_orders.values()];
      sorted_sell.sort(function(a, b){ if (a.price > b.price) return 1; else return -1;});
      for(let i=0; i < sorted_sell.length; i++)
      {
        sum += sorted_sell[i].price * (sorted_sell[i].qty - sorted_sell[i].cum_qty);
        if (sum > this.min_balance)
          max_sell_price = sorted_sell[i].price;
      }

      if (max_sell_price === Number.MIN_VALUE || min_buy_price === Number.MAX_VALUE) {
        this.initialize();
        return this.rate();
      }

      let d2 = 0.0;
      let d1 = max_sell_price - min_buy_price;
      if (this.market === 'US')
        d2 = max_sell_price + min_buy_price;

      let gap = d1*2/d2;
      if (gap <= this.min_spread) {
        this.T += 1;
        console.log("T: " + this.T);
       }

      this.initialize();
      return this.rate();
  },

  serialize: function() {return '';},

  deserialize: function (state_str) {},

  merge: function() {}
}
$$;

