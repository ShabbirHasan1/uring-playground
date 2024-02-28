use std::{
    io::Result,
    net::{Ipv6Addr, SocketAddr, TcpListener},
    os::fd::AsFd,
    rc::Rc,
};

use hyper::{header::CONTENT_TYPE, server::conn::http1::Builder, Response, StatusCode};
use hyper_util::rt::TokioIo;
use io_uring::IoUring;
use uring_playground::{Executor, PollIo, Reactor};

fn main() -> Result<()> {
    let reactor = IoUring::new(256).map(Reactor::new).map(Rc::new)?;
    let executor = Rc::new(Executor::new());

    let listener = TcpListener::bind(SocketAddr::from((Ipv6Addr::UNSPECIFIED, 8080)))?;
    listener.set_nonblocking(true)?;

    let address = listener.local_addr()?;
    println!("listening on {address}");

    let task = executor.spawn(async {
        loop {
            let (stream, address) = uring_playground::accept(&reactor, listener.as_fd()).await?;

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

    uring_playground::block_on(task, || {
        executor.tick();
        reactor.tick()
    })?
}
