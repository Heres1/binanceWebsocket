//! 实时数据录制器
//!
//! 订阅WebSocket数据，录制到本地JSONL文件，用于后续回放回测

use crate::backtest::data_loader::RecordedEvent;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

/// 数据录制器
pub struct DataRecorder {
    file_path: String,
}

impl DataRecorder {
    /// 创建录制器，自动按日期生成文件名
    pub fn new(data_dir: &str, symbol: &str) -> Self {
        let dir = format!("{}/recorded", data_dir);
        fs::create_dir_all(&dir).ok();

        let date = chrono::Utc::now().format("%Y%m%d").to_string();
        let file_path = format!("{}/{}_{}.jsonl", dir, symbol, date);

        println!("🔴 录制器启动: {}", file_path);

        Self { file_path }
    }

    /// 录制一个事件
    pub fn record(&self, event: &RecordedEvent) {
        match serde_json::to_string(event) {
            Ok(json) => {
                if let Ok(mut file) = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.file_path)
                {
                    let _ = writeln!(file, "{}", json);
                }
            }
            Err(e) => eprintln!("录制序列化失败: {}", e),
        }
    }

    /// 获取录制文件路径
    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    /// 检查录制文件是否存在
    pub fn has_recording(&self) -> bool {
        Path::new(&self.file_path).exists()
    }
}
