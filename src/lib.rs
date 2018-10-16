//! A simple library for a tcp socket (or any other async read/write) that allows input to flow to different
//! channels.

extern crate bytes;
extern crate futures;
extern crate tokio_codec;
extern crate tokio_core;
extern crate tokio_io;
#[macro_use]
extern crate log;
extern crate log4rs;

use futures::{Future, Sink, Stream};
use futures::future::{self, Either, Loop};
use futures::unsync::mpsc::{channel, Receiver, Sender};
use std::io;
use std::error::Error;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_codec::{Decoder, Encoder, FramedRead, FramedWrite};
use tokio_io::io::{ReadHalf, WriteHalf};

mod coder;

/// The kind of future used by a spork.
pub type SporkFuture<I> = Box<Future<Item = I, Error = io::Error>>;

/// The read half of a stream used by a spork.
pub type SporkRead<T, D> = FramedRead<ReadHalf<T>, D>;

/// The write half of a stream used by a spork.
pub type SporkWrite<T, E> = FramedWrite<WriteHalf<T>, E>;

/// The underlying read operation, which drives all dispatch and processing of messages.
pub type ReadFuture<T, D> = Box<Future<Item = SporkRead<T, D>, Error = (io::Error, SporkRead<T, D>)>>;

/// The underlying write operation, which drives sending data to the socket.
type WriteFuture<T, E> = Box<Future<Item = SporkWrite<T, E>, Error = (io::Error, Option<SporkWrite<T, E>>)>>;

/// The type of future that drives the responder, which will complete only once the responder has finished
/// responding to all items.
pub type RespFuture = Box<Future<Item = (), Error = io::Error>>;

/// The underlying write operation, which drives sending data to the socket.
pub type RwFuture = Box<Future<Item = (), Error = io::Error>>;

/// The channels used by a spork, which determines which process handles an incoming message. Any incoming
/// message is handled immediately and done, or is handled as part of a ongoing conversation which started
/// either on this side of the socket (`Command`) or on the other side (`Respond`).
pub enum Channel {
  Immediate,
  Command,
  Signal,
  Respond
}

/// A managed connection to a socket or other read/write data, which consists of:
/// - The actual underlying readable/writeable data
/// - A decoder to convert raw incoming data into objects
/// - An encoder to convert outgoing objects into data
/// - A *selector function* to determine which channel a message will be processed on.
/// - An *immediate function* which handles all immediate message processing.
/// - A *responder function* which constructs a responer.
pub struct Spork<T, D, E, S, H, N, Q, R>
where
  T: AsyncRead + AsyncWrite + Sized,
  D: Decoder + 'static,
  D::Error: Into<io::Error>,
  E: Encoder + 'static,
  S: Fn(&D::Item) -> Channel,
  H: FnOnce(Sender<E::Item>) -> N + 'static,
  N: Responder<D> + 'static,
  Q: FnOnce(Chatter<D, E>) -> R,
  R: Responder<D>
{
  name: String,
  read: SporkRead<T, D>,
  write: SporkWrite<T, E>,
  s: S,
  h: H,
  q: Q
}

impl<T, D, E, S, H, N, Q, R> Spork<T, D, E, S, H, N, Q, R>
where
  T: AsyncRead + AsyncWrite + Sized + 'static,
  D: Decoder + 'static,
  D::Error: Into<io::Error>,
  E: Encoder + 'static,
  E::Error: Into<io::Error>,
  S: Fn(&D::Item) -> Channel + 'static,
  H: FnOnce(Sender<E::Item>) -> N + 'static,
  N: Responder<D> + 'static,
  Q: FnOnce(Chatter<D, E>) -> R,
  R: Responder<D> + 'static
{
  /// Create a new spork which processes data.
  pub fn new(name: String, t: T, d: D, e: E, s: S, h: H, q: Q) -> Spork<T, D, E, S, H, N, Q, R> {
    let (r, w) = t.split();
    let read = FramedRead::new(r, d);
    let write = FramedWrite::new(w, e);
    Spork { name, read, write, s, h, q }
  }

  /// Process messages coming from the server connection. This method returns three things:
  ///
  /// - A *read/write future*, which dispatches and sends messages to and from the underlying socket. This
  ///   future acts a "pump" for all interactions with the target.
  ///
  ///   This future should run concurrently with any `send` or `ask` method in the command chatter (also
  ///   returned). It will successfully complete only when either the read or write communication to the remote
  ///   socket is closed. When it does complete, it yields:
  ///   - On success: a read half and write half of the original socket.
  ///   - On failure: an I/O error for failure, along with the read half and (possible) write half of the
  ///     socket. You will not receive the write half of the socket if it was dropped by the error.
  /// - A `Chatter` object that can be used to initiate conversations from this side of the connection. The
  ///   `send` and `ask` methods of this chat return a future, which will not complete unless the read/write
  ///   future is also being executed.
  /// - A *responder future*, which continuously loops the `Responder::respond()` function, only completing once
  ///   there are no more conversations from the other side of the connection to respond to. This is also driven
  ///   from the returned read/write future.
  pub fn process(self) -> (RwFuture, Chatter<D, E>, RespFuture) {
    let (name, read, write, s, h, q) = (self.name, self.read, self.write, self.s, self.h, self.q);
    let (here_s, here_r) = channel(2);
    let (there_s, there_r) = channel(2);
    let (sgnl_s, sgnl_r) = channel(2);

    let (write_in, write_ftr) = pump(write);
    let cmdr = Chatter::new(write_in.clone(), here_r);
    let snpr = h(write_in.clone());
    let rspr = Chatter::new(write_in, there_r);
    let dspr = Dispatcher::new(&name, read, s, snpr, here_s, sgnl_s, there_s);
    let read_ftr = Box::new(dspr.dispatch());

    let read_ftr = read_ftr.map(DoneType::Read).map_err(|(e, read)| (e, Some(DoneType::Read(read))));
    let write_ftr = write_ftr.map(DoneType::Write).map_err(|(e, write)| (e, write.map(DoneType::Write)));

    // We can't allow the read to complete if write completes (or vice versa), since it's likely that it will
    // continue forever, never completing with its error.
    let rw_ftr = read_ftr.select(write_ftr).then(|v| match v {
      Ok((_first, _second)) => {
        debug!("rw_ftr done at OK");
        future::ok(())
      },
      Err(((e1, _first), _second)) => {
        debug!("rw_ftr done at Err");
        future::err(e1)
      }
    });

    let resp = q(rspr);

    let resp_ftr = future::loop_fn((sgnl_r, resp), move |(sgnl_r, resp)| {
      sgnl_r.into_future().then(move |v| match v {
        Err(_) => Either::B(future::err(io_err("Lost signal."))),
        Ok((oval, sgnl_r)) => match oval {
          None => Either::B(future::ok(Loop::Break(()))),
          Some(val) => Either::A(resp.respond(val).map(|(_, resp)| Loop::Continue((sgnl_r, resp))))
        }
      })
    });

    (Box::new(rw_ftr), cmdr, Box::new(resp_ftr))
  }
}

enum DoneType<T, D, E> {
  Read(SporkRead<T, D>),
  Write(SporkWrite<T, E>)
}

fn pump<T, E>(write: SporkWrite<T, E>) -> (Sender<E::Item>, WriteFuture<T, E>)
where
  T: AsyncRead + AsyncWrite + Sized + 'static,
  E: Encoder + 'static,
  E::Error: Into<io::Error>
{
  let (send, rcv) = channel(2);

  let pump_ftr = Box::new(future::loop_fn((rcv, write), move |(rcv, write)| {
    rcv.into_future().then(move |v| match v {
      Err(_) => Either::B(future::err((io_err("dropped"), Some(write)))),
      Ok((oval, rcv)) => match oval {
        Some(val) => Either::A(write.send(val).map_err(|e| (e.into(), None)).map(|write| Loop::Continue((rcv, write)))),
        None => Either::B(future::ok(Loop::Break(write)))
      }
    })
  }));

  (send, pump_ftr)
}

struct Dispatcher<T, D, S, N>
where
  T: AsyncRead + 'static,
  D: Decoder + 'static,
  D::Error: Into<io::Error>,
  S: Fn(&D::Item) -> Channel + 'static,
  N: Responder<D> + 'static
{
  name: String,
  read: SporkRead<T, D>,
  s: S,
  n: N,
  here: Sender<D::Item>,
  there: Sender<D::Item>,
  sgnl: Sender<D::Item>
}

impl<T, D, S, N> Dispatcher<T, D, S, N>
where
  T: AsyncRead + 'static,
  D: Decoder + 'static,
  D::Error: Into<io::Error>,
  S: Fn(&D::Item) -> Channel + 'static,
  N: Responder<D> + 'static
{
  pub fn new(name: &str, read: FramedRead<ReadHalf<T>, D>, s: S, n: N, here: Sender<D::Item>, there: Sender<D::Item>,
             sgnl: Sender<D::Item>)
      -> Dispatcher<T, D, S, N> {
    let name = name.to_string();
    Dispatcher { name, read, s, n, here, there, sgnl }
  }

  /// Dispatch or otherwise react to a message received from a server.
  ///
  /// This sends all messages to a channel, where they are responded to appropriately.
  fn dispatch(self) -> ReadFuture<T, D> {
    Box::new(
      future::loop_fn(self, move |dspr| {
        dspr.into_future().and_then(move |(oreq, dspr)| match oreq {
          Some(val) => Either::A(dspr.handle_next(val).map(|(_, dspr)| Loop::Continue(dspr))),
          None => Either::B(future::ok(Loop::Break(dspr)))
        })
      }).then(move |v| match v {
        Ok(dspr) => future::ok(dspr.into_read()),
        Err((e, read)) => future::err((e, read))
      })
    )
  }

  /// Just like `Stream.into_future`, but for the dispatcher.
  fn into_future(self) -> DispFuture<Option<D::Item>, T, D, S, N> {
    let (name, read, s, n, here, there, sgnl) =
        (self.name, self.read, self.s, self.n, self.here, self.there, self.sgnl);

    Box::new(read.into_future().then(move |v| match v {
      Ok((v, read)) => future::ok((v, Dispatcher::new(&name, read, s, n, here, there, sgnl))),
      Err((e, read)) => future::err((e.into(), read))
    }))
  }

  /// Handles a single server message by acting directly (such as with `Data` or `Open`), or by routing it to
  /// the appropriate channel.
  fn handle_next(self, val: D::Item) -> DispFuture<(), T, D, S, N> {
    let (name, read, s, n, here, there, sgnl) =
        (self.name, self.read, self.s, self.n, self.here, self.there, self.sgnl);

    let channel = s(&val);
    match channel {
      Channel::Immediate => Box::new(n.respond(val).then(move |r| match r {
        Ok((_, n)) => future::ok(((), Dispatcher::new(&name, read, s, n, here, there, sgnl))),
        Err(e) => future::err((e, read))
      })),
      Channel::Command => Box::new(here.send(val).then(move |r| match r {
        Ok(here) => future::ok(((), Dispatcher::new(&name, read, s, n, here, there, sgnl))),
        Err(e) => future::err((io_err(e.description()), read))
      })),
      Channel::Signal => Box::new(sgnl.send(val).then(move |r| match r {
        Ok(sgnl) => future::ok(((), Dispatcher::new(&name, read, s, n, here, there, sgnl))),
        Err(e) => future::err((io_err(e.description()), read))
      })),
      Channel::Respond => Box::new(there.send(val).then(move |r| match r {
        Ok(there) => future::ok(((), Dispatcher::new(&name, read, s, n, here, there, sgnl))),
        Err(e) => future::err((io_err(e.description()), read))
      }))
    }
  }

  fn into_read(self) -> SporkRead<T, D> { self.read }
}

type DispFuture<I, T, D, S, N> = Box<Future<Item = (I, Dispatcher<T, D, S, N>), Error = (io::Error, SporkRead<T, D>)>>;

pub struct Chatter<D, E>
where
  D: Decoder + 'static,
  E: Encoder + 'static
{
  write: Sender<E::Item>,
  read: Receiver<D::Item>
}

impl<D, E> Chatter<D, E>
where
  D: Decoder,
  E: Encoder
{
  fn new(write: Sender<E::Item>, read: Receiver<D::Item>) -> Chatter<D, E> {
    Chatter { write, read }
  }

  /// Send a message to the server, without expecting a particular response.
  pub fn send(self, msg: E::Item) -> ChatFuture<(), D, E> {
    let (write, read) = (self.write, self.read);
    Box::new(write.send(msg).map_err(|e| io_err(e.description())).map(move |write| ((), Chatter::new(write, read))))
  }

  /// Gets a clone of the sender, which can be used to send messages to the chat target without consuming this
  /// chatter.
  pub fn clone_sender(&self) -> Sender<E::Item> {
    self.write.clone()
  }

  /// Send a message to the server, and return a future that has the response message.
  pub fn ask(self, msg: E::Item) -> ChatFuture<D::Item, D, E> {
    let (write, read) = (self.write, self.read);
    Box::new(write.send(msg).map_err(|e| io_err(e.description())).and_then(move |write| {
      read.into_future().map_err(|_| io_err("Dropped answer.")).and_then(move |(oval, read)| match oval {
        Some(val) => future::ok((val, Chatter::new(write, read))),
        None => future::err(io_err("Expected answer, got nothing."))
      })
    }))
  }
}

/// A commander future returns the commander upon success.
pub type ChatFuture<I, D, E> = Box<Future<Item = (I, Chatter<D, E>), Error = io::Error>>;

/// An object that responds to the start of a conversation initiated by the other side of a data connection.
pub trait Responder<D>
where
  D: Decoder
{
  fn respond(self, item: D::Item) -> RsprFuture<(), Self>;
}

/// The type of future returned by a responder, which includes the responder itself.
pub type RsprFuture<I, R> = Box<Future<Item = (I, R), Error = io::Error>>;

/// A quick and dirty way to create an `io::Error` from a string.
///
/// # Examples
///
/// ```
/// # use g2c::common::io_err;
/// # use std::error::Error;
/// let err = io_err("test");
/// assert_eq!(err.description(), "test");
/// ```
pub fn io_err(desc: &str) -> io::Error { io::Error::new(io::ErrorKind::Other, desc) }
