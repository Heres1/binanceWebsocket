pub mod order_commands;

use order_commands::PlaceOrderCommand;


#[derive(Debug,Clone)]
pub enum Command{
    PlaceOrder(PlaceOrderCommand),
}
impl Command{
    pub fn command_type(&self)->CommandType{
        match self{
            Command::PlaceOrder(_) => CommandType::PlaceOrder,
        }
    }
    pub fn command_name(&self)->&'static str{
        match self{
            Command::PlaceOrder(_) => "PlaceOrder",
        }
    }
}

#[derive(Debug,Clone,Copy,PartialEq,Eq,Hash)]
pub enum CommandType{
    // 订单命令
    PlaceOrder,
}
#[derive(Debug,Clone)]
pub enum CommandResult{
    Success{
        message:String,
        data:Option<String>,
    },
    Failure{
        reason:String,
        code:u32,
    },
    Accepted{
        tracking_id:String,
        estimated_completion:Option<String>,
    }
}
impl CommandResult{
    pub fn success(message:impl Into<String>)->Self{
        CommandResult::Success{
            message:message.into(),
            data:None,
        }
    }
    pub fn success_with_data(message:impl Into<String>, data:impl Into<String>)->Self{
        CommandResult::Success{
            message:message.into(),
            data:Some(data.into()),
        }
    }
    pub fn failure(reason:impl Into<String>, code:u32)->Self{
        CommandResult::Failure{
            reason:reason.into(),
            code,
        }
    }
    pub fn accepted(tracking_id:impl Into<String>, estimated_completion:Option<impl Into<String>>)->Self{
        CommandResult::Accepted{
            tracking_id:tracking_id.into(),
            estimated_completion:estimated_completion.map(|s|s.into()),
        }
    }
    pub fn is_success(&self)->bool{
        matches!(self,CommandResult::Success{..})
    }
    pub fn is_failure(&self)->bool{
        matches!(self,CommandResult::Failure{..})
    }
    pub fn is_accepted(&self)->bool{
        matches!(self,CommandResult::Accepted{..})
    }
}