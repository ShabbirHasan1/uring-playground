use std::{io::Result, net::SocketAddr, num::NonZeroUsize, os::fd::AsFd, rc::Rc};

use clap::Parser;
use hyper::{header::CONTENT_TYPE, server::conn::http1::Builder, Response, StatusCode};
use io_uring::IoUring;
use local_fifo_executor::Executor;
use socket2::{Protocol, SockAddr, Socket, Type};
use uring_adapter::PollIo;
use uring_reactor::Reactor;

#[derive(Debug, Parser)]
struct Arguments {
    #[arg(long, env, default_value = "2")]
    workers: NonZeroUsize,
    #[arg(long, env, default_value = "256")]
    entries: u32,
    #[arg(long, env, default_value = "512")]
    backlog: i32,
    #[arg(long, env, default_value = "[::]:8080")]
    address: SocketAddr,
}

fn start(arguments: &Arguments, index: usize) -> Result<()> {
    let executor = Rc::new(Executor::new());
    let reactor = IoUring::new(arguments.entries)
        .map(Reactor::new)
        .map(Rc::new)?;

    let address = SockAddr::from(arguments.address);
    let socket = Socket::new(
        address.domain(),
        Type::STREAM.nonblocking(),
        Some(Protocol::TCP),
    )?;

    socket.set_nonblocking(true)?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.bind(&address)?;
    socket.listen(arguments.backlog)?;

    let actual = socket.local_addr()?.as_socket().unwrap();
    println!("worker {index} listening on {actual}");

    let task = executor.spawn(async {
        loop {
            let (stream, address) = uring_operation::accept(&reactor, socket.as_fd()).await?;

            let address = address.as_socket().unwrap();
            let connection = Builder::new().serve_connection(
                PollIo::new(reactor.clone(), stream),
                hyper::service::service_fn(move |_| async move {
                    let message = format!("Hello to you {address} from worker {index}\n");

                    Response::builder()
                        .status(StatusCode::OK)
                        .header(CONTENT_TYPE, "text/plain")
                        .body(message)
                }),
            );

            executor
                .spawn(async {
                    if let Err(error) = connection.await {
                        eprintln!("connection error from {address}: {error}");
                    }
                })
                .detach();
        }
    });

    local_fifo_executor::block_on(task, || {
        executor.tick();
        reactor.tick()
    })?
}

fn main() -> Result<()> {
    let arguments = Arguments::parse();

    std::thread::scope(|scope| {
        (0..arguments.workers.get())
            .map(|index| {
                let arguments = &arguments;
                scope.spawn(move || start(arguments, index))
            })
            .collect::<Vec<_>>()
            .into_iter()
            .try_for_each(|thread| thread.join().unwrap())
    })
}
