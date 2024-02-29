use std::{
    io::Result,
    net::{SocketAddr, TcpListener},
    os::fd::AsFd,
    rc::Rc,
};

use clap::Parser;
use hyper::{header::CONTENT_TYPE, server::conn::http1::Builder, Response, StatusCode};
use hyper_util::rt::TokioIo;
use io_uring::IoUring;
use local_fifo_executor::Executor;
use uring_adapter::PollIo;
use uring_reactor::Reactor;

#[derive(Debug, Parser)]
struct Arguments {
    #[arg(long, env, default_value = "256")]
    entries: u32,
    #[arg(long, env, default_value = "[::]:8080")]
    address: SocketAddr,
}

fn main() -> Result<()> {
    let arguments = Arguments::parse();
    let executor = Rc::new(Executor::new());
    let reactor = IoUring::new(arguments.entries)
        .map(Reactor::new)
        .map(Rc::new)?;

    let listener = TcpListener::bind(arguments.address)?;
    listener.set_nonblocking(true)?;

    let address = listener.local_addr()?;
    println!("listening on {address}");

    let task = executor.spawn(async {
        loop {
            let (stream, address) = uring_operation::accept(&reactor, listener.as_fd()).await?;

            let address = address.as_socket().unwrap();
            let connection = Builder::new().serve_connection(
                TokioIo::new(PollIo::new(reactor.clone(), stream)),
                hyper::service::service_fn(move |_| async move {
                    let message = format!("Hello, there {address}!\n",);

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
