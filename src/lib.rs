use std::thread;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::collections::HashMap;
use std::fs;
use std::net::TcpListener;
use std::net::TcpStream;
use std::io::prelude::*;
use std::env;

/// Specifies the message type sent to workers. 
/// 
/// NewJob type contains the job to execute.
/// 
/// Terminate informs the worker to stop running.
enum Message {
    NewJob(Job),
    Terminate,
}


pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Message>,
}

impl ThreadPool {
    /// Create a new ThreadPool.
    ///
    /// The size is the number of threads in the pool.
    ///
    /// # Panics
    ///
    /// The `new` function will panic if the size is zero.
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);

        println!("Thread pool size is {}", size);

        let (sender, receiver) = mpsc::channel();

        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        ThreadPool {
            workers,
            sender,
        }
    }

    pub fn execute<F>(&self, f: F)
        where
            F: FnOnce() + Send + 'static
    {
        let job = Box::new(f);

        self.sender.send(Message::NewJob(job)).unwrap();
    }
}

/// Send Message::Terminate message to each worker before the pool gets dropped.
impl Drop for ThreadPool {
    fn drop(&mut self) {
        
        for _ in &mut self.workers {
            self.sender.send(Message::Terminate).unwrap();
        }

        for worker in &mut self.workers {

            println!("Shutting down worker {}", worker.id);
            
            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

trait FnBox {
    fn call_box(self: Box<Self>);
}

impl<F: FnOnce()> FnBox for F {
    fn call_box(self: Box<F>) {
        (*self)()
    }
}

type Job = Box<dyn FnBox + Send + 'static>;

struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) ->
        Worker {
        let thread = thread::spawn(move ||{
            loop {
                let message = receiver.lock().unwrap().recv().unwrap();
                match message {
                    Message::NewJob(job) => {
                        job.call_box();
                    },
                    Message::Terminate => {
                        break;
                    },
                }
            }
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }
}

pub struct Cache {
    cache: HashMap<String, String>
}

impl Cache {
    pub fn init(&mut self) -> Result<String, std::io::Error> {

        let files_to_load = vec!["C:\\Users\\crusn\\Rust\\cacheserver\\src\\res\\file1.txt", "C:\\Users\\crusn\\Rust\\cacheserver\\src\\res\\file2.txt"];

        self.load_data_to_cache(&files_to_load)?;
        
        let output_data = self.get_output();
        Ok(format!("loaded data - {}", output_data))
    }
    
    fn get_output(&mut self) -> String {
        let mut output_data = String::new();
        for (file, data) in &self.cache {
            output_data = output_data + file + "\t" + data;
        };
        output_data
    }

    fn load_data_to_cache(&mut self, files_to_load: &Vec<&str>) -> Result<(), std::io::Error> {
        for file in files_to_load.iter() {
            let content = fs::read_to_string(file)?;
            let file_path_vec: Vec<&str> = file.split("\\").collect();
            self.cache.insert(file_path_vec[file_path_vec.len() - 1].to_string(), content);
        }
        Ok(())
    }

    pub fn new() -> Cache {
        Cache {cache : HashMap::new()}
    }

    pub fn get(&self, key: &str) -> String {
        match self.cache.get(key) {
            Some(data) => data.clone(),
            None => String::from("File is not present in cache. Please run init or add the file to the list of files configured for caching."),
        }
    }
}

const INIT: &'static [u8; 20] = b"GET /init HTTP/1.1\r\n";
const GET: &'static [u8; 14] = b"GET /get?file=";
const NOT_FOUND: &str = "404 Not Found.";
const HTTP_OK: &str = "HTTP/1.1 200 OK\r\n\r\n";


/// Starts listening to an ip:port config for incoming HTTP requests.
/// 
/// Creates a thread pool to handle the incoming requests. Pool size is either passed as a command line argument or defaulted to four.
/// 
/// # Panics
///
/// The 'run' function will panic if pool size is not a valid number as part of the command line arguments or there are any errors binding to ip:port 
/// or processing incoming requests.
pub fn run() {
    let listener = TcpListener::bind("127.0.0.1:1337").unwrap();
    let pool = ThreadPool::new(get_thread_pool_size());
    let cache = Arc::new(Mutex::new(Cache::new()));

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let cache_clone = Arc::clone(&cache);
        pool.execute(move || {
            handle_connection(stream, cache_clone);
        });
    }
}

fn get_thread_pool_size() -> usize {
    let program_arguments: Vec<String> = env::args().collect();
    match program_arguments.get(1) {
        Some(pool_size) => pool_size.parse().unwrap(),
        None => 4
    }
}

/// Handles the incoming web requests.
///
/// Parses request as string into the following categories - INIT, GET, NOT_FOUND.
/// 
/// For INIT request types starts the loading of files into cache.
/// 
/// For GET request types loads the data from cache for the file name mentioned in the request and writes the text to response.
/// 
/// For NOT_FOUND returns an 404 not found response.
fn handle_connection(mut stream: TcpStream, cache: Arc<Mutex<Cache>>) {
    let mut buffer = [0; 512];
    stream.read(&mut buffer).unwrap();

    let (status_line, response_data) = if buffer.starts_with(GET) {

        let file_name = extract_file_name_from_request(&buffer);

        (HTTP_OK, cache.lock().unwrap().get(&file_name))

    } else if buffer.starts_with(INIT) {

        let result = match cache.lock().unwrap().init() {
            Ok(load_status) =>  load_status,
            Err(load_err) => load_err.to_string(),
        };

        (HTTP_OK, result)
    } else {

        ("HTTP/1.1 404 NOT FOUND\r\n\r\n", NOT_FOUND.to_string())

    };

    let response = format!("{}{}", status_line, response_data);

    stream.write(response.as_bytes()).unwrap();
    stream.flush().unwrap();
}

/// Handles the incoming web requests.
/// 
/// Extracts file name from the buffer array using a predefined text match logic. 
/// 
/// This is just a test code and should not be the final solution.
/// 
fn extract_file_name_from_request(buffer: &[u8]) -> String {
    let request_string = String::from_utf8_lossy(buffer);
    let vec: Vec<&str> = request_string.split("GET /get?file=").collect();
    let vec: Vec<&str> = vec[1].split_whitespace().collect();
    match vec.get(0) {
        Some(file_name) => file_name.to_string(),
        None => "File name cannot be extracted from the request.".to_string()
    }
}