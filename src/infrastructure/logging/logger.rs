use log::{LevelFilter, Metadata, Record};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::sync::mpsc;
use std::thread;
use std::path::{Path, PathBuf};
use chrono::Local;
use crate::error::InfrastructureError;
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
    file_path: Option<String>, // 日志基础路径（如 logs/trading.log）
    rotate_size: Option<u64>,  // 单次运行日志文件大小上限（字节）
    max_files: usize,        // 历史日志最大保留数
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
            rotate_size: Some(100 * 1024 * 1024), // 单文件100MB上限
            max_files: 10,                         // 保留最近10个历史日志
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

/// 生成带时间戳的日志文件路径
/// 输入: "logs/trading.log" → 输出: "logs/trading_2026-05-24_163506.log"
fn generate_session_log_path(base_path: &str) -> String {
    let path = Path::new(base_path);
    let stem = path.file_stem().unwrap_or_default().to_str().unwrap_or("trading");
    let ext = path.extension().unwrap_or_default().to_str().unwrap_or("log");
    let dir = path.parent().unwrap_or(Path::new("."));
    let timestamp = Local::now().format("%Y-%m-%d_%H%M%S");
    dir.join(format!("{}_{}.{}", stem, timestamp, ext)).to_string_lossy().to_string()
}

/// 清理历史日志，只保留最近 max_files 个
fn cleanup_old_logs(base_path: &str, max_files: usize) {
    let path = Path::new(base_path);
    let stem = path.file_stem().unwrap_or_default().to_str().unwrap_or("trading");
    let ext = path.extension().unwrap_or_default().to_str().unwrap_or("log");
    let dir = path.parent().unwrap_or(Path::new("."));
    
    // 收集匹配 trading_*.log 的文件
    let pattern = format!("{}_{}", stem, ""); // prefix: "trading_"
    let mut log_files: Vec<PathBuf> = Vec::new();
    
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let file_name = entry.file_name().to_string_lossy().to_string();
            if file_name.starts_with(&pattern) && file_name.ends_with(&format!(".{}", ext)) {
                log_files.push(entry.path());
            }
        }
    }
    
    // 按文件名排序（时间戳格式天然有序）
    log_files.sort();
    
    // 如果超过max_files个，删除最旧的
    if log_files.len() > max_files {
        let to_remove = log_files.len() - max_files;
        for old_file in log_files.iter().take(to_remove) {
            if let Err(e) = fs::remove_file(old_file) {
                eprintln!("清理旧日志失败 {:?}: {}", old_file, e);
            }
        }
    }
}

/// 运行内日志大小监控
struct SizeMonitor {
    current_path: String,
    max_size: u64,
    base_path: String,
}

impl SizeMonitor {
    fn new(current_path: String, max_size: u64, base_path: String) -> Self {
        SizeMonitor { current_path, max_size, base_path }
    }

    fn should_split(&self) -> bool {
        match fs::metadata(&self.current_path) {
            Ok(m) => m.len() > self.max_size,
            Err(_) => false,
        }
    }

    /// 当前文件超大时，切换到新文件
    fn split(&mut self) -> Option<File> {
        let new_path = generate_session_log_path(&self.base_path);
        match OpenOptions::new().create(true).append(true).open(&new_path) {
            Ok(file) => {
                self.current_path = new_path;
                Some(file)
            }
            Err(e) => {
                eprintln!("日志分割失败: {}", e);
                None
            }
        }
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
            let mut current_file: Option<File> = None;
            let mut size_monitor: Option<SizeMonitor> = None;
            
            if let Some(ref base_path) = config_clone.file_path {
                // 自动创建日志目录
                if let Some(parent) = Path::new(base_path).parent() {
                    if !parent.exists() {
                        if let Err(e) = fs::create_dir_all(parent) {
                            eprintln!("创建日志目录失败 {:?}: {}", parent, e);
                        }
                    }
                }
                
                // 清理历史日志
                cleanup_old_logs(base_path, config_clone.max_files);
                
                // 生成本次运行的日志文件名（带时间戳）
                let session_path = generate_session_log_path(base_path);
                
                match OpenOptions::new().create(true).append(true).open(&session_path) {
                    Ok(file) => {
                        current_file = Some(file);
                        
                        if let Some(max_size) = config_clone.rotate_size {
                            size_monitor = Some(SizeMonitor::new(
                                session_path.clone(), max_size, base_path.clone()
                            ));
                        }
                        
                        // 创建/更新软链接，方便 tail -f 查看最新日志
                        let _ = Self::update_symlink(base_path, &session_path);
                    },
                    Err(e) => {
                        eprintln!("打开日志文件失败 {}: {}", session_path, e);
                    }
                }
            }

            loop {
                match receiver.recv() {
                    Ok(msg) => {
                        let formatted_msg = Self::format_message(&config_clone, &msg);
                        
                        // 检查是否需要分割（单文件超过上限）
                        if let Some(ref mut monitor) = size_monitor {
                            if monitor.should_split() {
                                if let Some(new_file) = monitor.split() {
                                    current_file = Some(new_file);
                                    // 更新软链接指向新文件
                                    if let Some(ref base_path) = config_clone.file_path {
                                        let _ = Self::update_symlink(base_path, &monitor.current_path);
                                    }
                                }
                            }
                        }
                        
                        // 写入日志文件
                        if let Some(ref mut file) = current_file {
                            if writeln!(file, "{}", formatted_msg).is_err() {
                                eprintln!("日志写入失败");
                            }
                            let _ = file.flush();
                        }
                        
                        // 只有WARN和ERROR级别输出到控制台
                        if msg.level <= log::Level::Warn {
                            println!("{}", formatted_msg);
                        }
                    },
                    Err(_) => break,
                }
            }
        });

        AsyncLogger {
            config,
            sender: Some(sender),
            handle: Some(handle),
        }
    }

    /// 创建/更新软链接，让 trading.log 始终指向当前会话的日志文件
    fn update_symlink(link_path: &str, target_path: &str) -> std::io::Result<()> {
        let link = Path::new(link_path);
        // 删除旧的链接或文件
        if link.exists() || link.symlink_metadata().is_ok() {
            fs::remove_file(link)?;
        }
        // 创建相对路径的软链接
        let target = Path::new(target_path);
        let target_filename = target.file_name().unwrap_or_default();
        #[cfg(unix)]
        std::os::unix::fs::symlink(target_filename, link)?;
        #[cfg(not(unix))]
        {
            // Windows不支持symlink，复制文件名到一个标记文件
            fs::write(link, target_filename.to_string_lossy().as_bytes())?;
        }
        Ok(())
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
                // 精简格式：[MM-DD HH:MM:SS] LEVEL - 消息
                let short_timestamp = Local::now().format("%m-%d %H:%M:%S").to_string();
                format!("[{}] {} - {}", short_timestamp, msg.level, msg.args)
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
        if metadata.level() > self.config.level {
            return false;
        }
        // debug/trace 级别：只输出本 crate 的日志，过滤第三方库噪声（tokio/hyper/reqwest 等）
        if metadata.level() > log::Level::Info {
            let target = metadata.target();
            return target.starts_with("rust_binance_event_driven")
                || target.starts_with("trading")
                || target.starts_with("backtest");
        }
        true
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
        assert_eq!(config.max_files(), 10);
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