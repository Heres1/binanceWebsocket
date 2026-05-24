use log::{LevelFilter, Metadata, Record};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::mpsc;
use std::thread;
use std::path::Path;
use chrono::Local;
use crate::infrastructure::error::error::InfrastructureError;
type Result<T> = std::result::Result<T, InfrastructureError>;

#[derive(Debug, Clone, PartialEq)]
pub enum LogFormat {
    Json,
    Text,
}

#[derive(Clone)]
pub struct LoggerConfig {
    level: LevelFilter,      // 日志级别
    format: LogFormat,       // 日志格式
    file_path: Option<String>, // 日志路径
    rotate_size: Option<u64>,  // 日志轮转大小（字节）
    max_files: usize,        // 最大文件数
    buffer_size: Option<usize>, // 缓冲区大小
}

impl LoggerConfig {
    pub fn new(
        level: LevelFilter,
        format: LogFormat,
        file_path: Option<String>,
        rotate_size: Option<u64>,
        max_files: usize,
    ) -> Self {
        LoggerConfig {
            level,
            format,
            file_path,
            rotate_size,
            max_files,
            buffer_size: Some(1024), // 默认缓冲区大小
        }
    }

    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = Some(buffer_size);
        self
    }

    pub fn default() -> Self {
        LoggerConfig {
            level: LevelFilter::Info,
            format: LogFormat::Text,
            file_path: None,
            rotate_size: Some(400 * 1024 * 1024), // 默认400MB轮转
            max_files: 5,
            buffer_size: Some(1024),
        }
    }

    pub fn level(&self) -> &LevelFilter {
        &self.level
    }
    pub fn format(&self) -> &LogFormat {
        &self.format
    }
    pub fn file_path(&self) -> &Option<String> {
        &self.file_path
    }
    pub fn rotate_size(&self) -> &Option<u64> {
        &self.rotate_size
    }
    pub fn max_files(&self) -> usize {
        self.max_files
    }
    pub fn buffer_size(&self) -> usize {
        self.buffer_size.unwrap_or(1024)
    }
}

// 日志轮转处理器
struct LogRotator {
    base_path: String,
    max_size: u64,
    max_files: usize,
}

impl LogRotator {
    fn new(base_path: String, max_size: u64, max_files: usize) -> Self {
        LogRotator {
            base_path,
            max_size,
            max_files,
        }
    }

    fn should_rotate(&self) -> bool {
        match std::fs::metadata(&self.base_path){
            Ok(metadata) => {
                if metadata.is_file(){
                    metadata.len() > self.max_size
                }else{
                    false
                }
                }
            Err(_) => false,
        }
    }

    fn rotate(&self) -> Result<()> {
        let oldest = format!("{}.{}", self.base_path, self.max_files);
        if Path::new(&oldest).exists() {
            std::fs::remove_file(&oldest)
                .map_err(|e| InfrastructureError::io_with_operation("删除最旧日志文件", e))?;
        }
        // 从最后一个文件开始向前重命名
        for i in (1..self.max_files).rev() {
            let old_name = format!("{}.{}", self.base_path, i);
            let new_name = format!("{}.{}", self.base_path, i + 1);
            // 重命名旧文件
            if Path::new(&old_name).exists() {
                std::fs::rename(&old_name, &new_name)
                    .map_err(|e| InfrastructureError::io_with_operation(
                        format!("重命名日志文件 {} -> {}", old_name, new_name), e
                    ))?;
            }
        }
        //将当前日志文件重命名为 .1
        let backup_name = format!("{}.1", self.base_path);
        if Path::new(&self.base_path).exists() {
            std::fs::rename(&self.base_path, &backup_name)
                .map_err(|e| InfrastructureError::io_with_operation(
                    format!("备份日志文件 {} -> {}", self.base_path, backup_name), e
                ))?;
        }
        Ok(())
    }
}

pub struct AsyncLogger {
    config: LoggerConfig,
    sender: Option<mpsc::Sender<LogMessage>>,
    handle: Option<thread::JoinHandle<()>>,
}

#[derive(Debug)]
struct LogMessage {
    level: log::Level,
    args: String,
    module_path: Option<String>,
    file: Option<String>,
    line: Option<u32>,
}

impl AsyncLogger {
    pub fn new(config: LoggerConfig) -> Self {
        let (sender, receiver) = mpsc::channel::<LogMessage>();
        
        let config_clone = config.clone();
        let handle = thread::spawn(move || {
            // 创建初始日志文件
            let mut current_file: Option<File> = None;
            let mut rotator: Option<LogRotator> = None;
            
            if let Some(ref file_path) = config_clone.file_path {
                // 自动创建日志目录
                if let Some(parent) = Path::new(file_path).parent() {
                    if !parent.exists() {
                        if let Err(e) = std::fs::create_dir_all(parent) {
                            eprintln!("Failed to create log directory {:?}: {}", parent, e);
                        }
                    }
                }
                
                // 尝试打开日志文件
                match OpenOptions::new().create(true).append(true).open(file_path) {
                    Ok(file) => {
                        current_file = Some(file);
                        
                        if let Some(size) = config_clone.rotate_size {
                            rotator = Some(LogRotator::new(file_path.clone(), size, config_clone.max_files));
                        }
                    },
                    Err(e) => {
                        eprintln!("Failed to open log file {}: {}", file_path, e);
                    }
                }
            }

            loop {
                match receiver.recv() {
                    Ok(msg) => {
                        let formatted_msg = Self::format_message(&config_clone, &msg);
                        
                        // 检查是否需要轮转
                        if let Some(ref rotator_val) = rotator {
                            if rotator_val.should_rotate() {
                                if let Err(e) = rotator_val.rotate() {
                                    eprintln!("Log rotation failed: {}", e);
                                }
                                
                                // 重新打开文件
                                if let Some(ref file_path) = config_clone.file_path {
                                    match OpenOptions::new().create(true).append(true).open(file_path) {
                                        Ok(new_file) => {
                                            current_file = Some(new_file);
                                        },
                                        Err(e) => {
                                            eprintln!("Failed to reopen log file {}: {}", file_path, e);
                                        }
                                    }
                                }
                            }
                        }
                        
                        // 写入日志文件
                        if let Some(ref mut file) = current_file {
                            if writeln!(file, "{}", formatted_msg).is_err() {
                                eprintln!("Failed to write to log file");
                            }
                            
                            // 立即刷新，确保日志及时写入
                            if file.flush().is_err() {
                                eprintln!("Failed to flush log file");
                            }
                        }
                        
                        // 只有WARN和ERROR级别输出到控制台（避免stdout.log过大）
                        if msg.level <= log::Level::Warn {
                            println!("{}", formatted_msg);
                        }
                    },
                    Err(_) => {
                        // 接收器已关闭，退出线程
                        break;
                    }
                }
            }
        });

        AsyncLogger {
            config,
            sender: Some(sender),
            handle: Some(handle),
        }
    }

    fn format_message(config: &LoggerConfig, msg: &LogMessage) -> String {
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        
        match config.format {
            LogFormat::Json => {
                let mut json_str = format!(
                    r#"{{"timestamp":"{}","level":"{}","message":"{}","#,
                    timestamp, msg.level, msg.args
                );
                                        
                if let Some(ref module) = msg.module_path {
                    json_str.push_str(&format!(r#""module":"{}","#, module));
                }
                                        
                if let Some(ref file) = msg.file {
                    json_str.push_str(&format!(r#""file":"{}","#, file));
                }
                                        
                if let Some(line) = msg.line {
                    json_str.push_str(&format!(r#""line":{}"#, line));
                } else {
                    // 移除最后的逗号
                    if json_str.ends_with(',') {
                        json_str.pop();
                    }
                }
                                        
                json_str.push('}');
                json_str
            },
            LogFormat::Text => {
                let mut text = format!("[{}] {} [{}] - {}", 
                                      timestamp, msg.level, std::thread::current().name().unwrap_or("<unnamed>"), msg.args);
                                        
                if let Some(ref module) = msg.module_path {
                    text.push_str(&format!(" (module: {})", module));
                }
                                        
                if let Some(ref file) = msg.file {
                    text.push_str(&format!(" @ {}:{}", file, msg.line.unwrap_or(0)));
                }
                                        
                text
            }
        }
    }

    pub fn init(config: LoggerConfig) -> Result<()> {
        let logger = Box::new(AsyncLogger::new(config));
        let level = logger.config.level; // 在logger被移动前保存level
        
        log::set_boxed_logger(logger)?;
        log::set_max_level(level);
        Ok(())
    }
}

impl Drop for AsyncLogger {
    fn drop(&mut self) {
        // 关闭发送端，让接收线程自然退出
        drop(self.sender.take());
        
        // 等待日志线程结束
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl log::Log for AsyncLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.config.level
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let msg: LogMessage = LogMessage {
                level: record.level(),
                args: record.args().to_string(),
                module_path: record.module_path().map(|s| s.to_string()),
                file: record.file().map(|s| s.to_string()),
                line: record.line(),
            };

            // 发送日志消息到异步处理线程
            if let Some(ref sender) = self.sender {
                if sender.send(msg).is_err() {
                    // 如果发送失败，直接打印到标准输出作为备选
                    eprintln!("Failed to send log message to async handler, falling back to stderr");
                    let fallback_msg = format!("[{}] [{}] - {}", 
                                             chrono::Local::now().format("%Y-%m-%d %H:%M:%S"), 
                                             record.level(), 
                                             record.args());
                    eprintln!("{}", fallback_msg);
                }
            }
        }
    }

    fn flush(&self) {
        // 异步日志记录器在每次写入后立即刷新，所以这里不需要额外操作
    }
}

#[cfg(test)]
mod tests{
    use super::*;

    #[test]
    fn test_logger() {
        let config = LoggerConfig::default();
        AsyncLogger::init(config).unwrap();
        log::info!("This is a test log message.");
        println!("This is a test log message.");
    }

    #[test]
    fn test_logger_config_default() {
        let config = LoggerConfig::default();
        assert_eq!(config.level(), &LevelFilter::Info);
        assert_eq!(config.format(), &LogFormat::Text);
        assert!(config.file_path().is_none());
        assert_eq!(config.max_files(), 5); // 更新默认值
        println!("Config test passed with values: level={:?}, format={:?}", config.level(), config.format());
    }

    #[test]
    fn test_logger_config_new() {
        let config = LoggerConfig::new(
            LevelFilter::Debug,
            LogFormat::Text, // 使用Text格式避免JSON解析复杂性
            None, // 不写入文件，只输出到控制台
            None,
            5
        );
        assert_eq!(config.level(), &LevelFilter::Debug);
        assert_eq!(config.format(), &LogFormat::Text);
        assert_eq!(config.max_files(), 5);
        println!("Custom config test passed: level={:?}, format={:?}", config.level(), config.format());
    }

    #[test]
    fn test_async_logger_creation() {
        let config = LoggerConfig::new(
            LevelFilter::Info,
            LogFormat::Text,
            None,
            None,
            3
        );
        let logger = AsyncLogger::new(config);
        println!("Logger instance created successfully");
        assert_eq!(logger.config.level, LevelFilter::Info);
    }

    // 只保留一个初始化测试，避免重复初始化问题
    #[test]
    fn test_logger_format_functions() {
        let config = LoggerConfig::new(
            LevelFilter::Info,
            LogFormat::Text,
            None,
            Some(1024 * 1024), // 1MB
            3
        );
        
        let msg = LogMessage {
            level: log::Level::Info,
            args: "Test message".to_string(),
            module_path: Some("test_module".to_string()),
            file: Some("test.rs".to_string()),
            line: Some(10),
        };
        
        let formatted_text = AsyncLogger::format_message(&config, &msg);
        println!("Formatted text: {}", formatted_text);
        assert!(formatted_text.contains("INFO"));  // 修复：移除方括号，因为实际输出格式是 "INFO"而不是 "[INFO]"
        assert!(formatted_text.contains("Test message"));
    }
}