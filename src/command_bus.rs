use crate::commands::*;
use crate::event_bus::TokioEventBus;
use crate::handlers::OrderCommandHandler;
use std::sync::Arc;
pub struct CommandBus{
    order_handler:OrderCommandHandler,
}
impl CommandBus{
    pub fn new(event_bus:Arc<TokioEventBus>)->Self{
        Self{
            order_handler:OrderCommandHandler::new(event_bus),
        }
    }
    pub async fn send(&self, command: Command) -> CommandResult {
        match command {
            Command::PlaceOrder(cmd) => self.order_handler.handle(cmd).await,
        }
    }
}