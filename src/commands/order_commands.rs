use std::fmt;

#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum OrderSide{
    Buy,
    Sell,
}

impl fmt::Display for OrderSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderSide::Buy => write!(f, "BUY"),
            OrderSide::Sell => write!(f, "SELL"),
        }
    }
}

#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum OrderType{
    Limit,
    Market,
    StopLoss,
    TakeProfit,
}

impl fmt::Display for OrderType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderType::Limit => write!(f, "LIMIT"),
            OrderType::Market => write!(f, "MARKET"),
            OrderType::StopLoss => write!(f, "STOP_LOSS"),
            OrderType::TakeProfit => write!(f, "TAKE_PROFIT"),
        }
    }
}
#[derive(Debug,Clone)]
pub struct PlaceOrderCommand{
    pub symbol:String,
    pub side:OrderSide,
    pub order_type:OrderType,
    pub price:Option<f64>,
    pub quantity:f64,
    pub client_order_id:Option<String>,
}
impl PlaceOrderCommand {
    pub fn buy_limit(symbol: impl Into<String>, price: f64, quantity: f64) -> Self {
        Self {
            symbol: symbol.into(),
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            price: Some(price),
            quantity,
            client_order_id: None,
        }
    }
    
    pub fn sell_limit(symbol: impl Into<String>, price: f64, quantity: f64) -> Self {
        Self {
            symbol: symbol.into(),
            side: OrderSide::Sell,
            order_type: OrderType::Limit,
            price: Some(price),
            quantity,
            client_order_id: None,
        }
    }
    
    pub fn with_client_order_id(mut self, id: impl Into<String>) -> Self {
        self.client_order_id = Some(id.into());
        self
    }
}