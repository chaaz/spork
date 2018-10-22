//! A communication protocol that directs messages to ongoing registered conversations. Using a spork lets you
//! run multiple conversions with the other side of a connection simultaneously, and you can write them all
//! using async futures-style code.
//!
//! ```
//! # extern crate bytes;
//! # extern crate futures;
//! # extern crate spork;
//! # extern crate tokio_core;
//! # extern crate tokio_io;
//! # use bytes::{Buf, BufMut, BytesMut, IntoBuf};
//! # use futures::{Async, Future, Stream};
//! # use futures::future::{self, Either};
//! # use futures::unsync::mpsc::{channel, Sender};
//! # use spork::*;
//! # use std::io::{self, Read, Write};
//! # use std::ops::{Deref, DerefMut};
//! # use tokio_core::reactor::Core;
//! # use tokio_core::reactor::Handle;
//! # use tokio_io::codec::{Decoder, Encoder};
//! # use tokio_io::{AsyncRead, AsyncWrite};
//! # struct Msg { msg: String }
//! # impl Msg {
//! #   fn new(msg: &str) -> Msg { Msg { msg: msg.into() } }
//! #   fn text(&self) -> &str { &self.msg }
//! # }
//! # struct Enc {}
//! # impl Enc { fn new() -> Enc { Enc {} } }
//! # impl Encoder for Enc { 
//! #   type Item = Msg;
//! #   type Error = io::Error;
//! #   fn encode(&mut self, msg: Msg, buf: &mut BytesMut) -> io::Result<()> {
//! #     buf.put_u32_be(msg.text().len() as u32);
//! #     buf.extend(msg.text().as_bytes());
//! #     Ok(())
//! #   }
//! # }
//! # struct Dec { len: Option<u32> }
//! # impl Dec { fn new() -> Dec { Dec { len: None } } }
//! # impl Decoder for Dec {
//! #   type Item = Msg;
//! #   type Error = io::Error;
//! #   fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Msg>> {
//! #     if let Some(len) = self.len {
//! #       if (buf.len() as u32) < len {
//! #         Ok(None)
//! #       } else {
//! #         Ok(Some(Msg::new(::std::str::from_utf8(&buf.split_to(len as usize).to_vec()).unwrap())))
//! #       }
//! #     } else {
//! #       if buf.len() < 4 {
//! #         Ok(None)
//! #       } else {
//! #         self.len = Some(buf.split_to(4).into_buf().get_u32_be());
//! #         self.decode(buf)
//! #       }
//! #     }
//! #   }
//! # }
//! # struct Rw { b: BytesMut }
//! # impl Rw { fn new() -> Rw { Rw { b: BytesMut::new() } } }
//! # impl Read for Rw {
//! #   fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
//! #     self.b.deref().read(buf)
//! #   }
//! # }
//! # impl AsyncRead for Rw {}
//! # impl Write for Rw {
//! #   fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
//! #     self.b.deref_mut().write(buf)
//! #   }
//! #   fn flush(&mut self) -> io::Result<()> { Ok(()) }
//! # }
//! # impl AsyncWrite for Rw { fn shutdown(&mut self) -> io::Result<Async<()>> { Ok(Async::Ready(())) } }
//! # fn io_err(desc: &str) -> io::Error { io::Error::new(io::ErrorKind::Other, desc) }
//! # fn judge_protocol_2(resp: Response<Dec, Enc>) -> impl Future<Item = bool, Error = io::Error> {
//! #   future::ok(true)
//! # }
//! # fn main() {
//! # let encoder = Enc::new();
//! # let decoder = Dec::new();
//! # let core = Core::new().unwrap();
//! # let handle = core.handle();
//! # let socket = Rw::new();
//! let spork = Spork::new("demo".into(), handle.clone(), socket, decoder, encoder, true);
//! let (chat, incoming_msgs) = spork.process();
//!
//! let outgoing_conversation = chat
//!   .ask(Msg::new("Sending 1"))
//!   .and_then(|response| {
//!     match response.message().text() {
//!       "Got 1" => future::ok(response),
//!       _ => future::err(io_err("Bad response 1."))
//!     }
//!   })
//!   .and_then(|response| {
//!     response.ask(Msg::new("Sending 2"))
//!   })
//!   .and_then(|response| {
//!     match response.message().text() {
//!       "Got 2" => future::ok(()),
//!       _ => future::err(io_err("Bad response 2."))
//!     }
//!   })
//!   .map(|_| println!("Done with protocol."))
//!   .map_err(|e| println!("Got error: {:?}.", e));
//!
//! let incoming_conversations = incoming_msgs
//!   .for_each(|response| {
//!     match response.message().text() {
//!       "Protocol_1" => Either::A(future::ok(true)),
//!       "Protocol_2" => Either::B(judge_protocol_2(response)),
//!       _ => Either::A(future::err(io_err("Unknown protocol.")))
//!     }
//!     .map(|v| println!("Judgement: {}", v))
//!   })
//!   .map(|_| println!("Done with incoming."))
//!   .map_err(|e| println!("Got error: {:?}", e));
//!
//! handle.spawn(outgoing_conversation);
//! handle.spawn(incoming_conversations);
//! # }
//! ```

extern crate bytes;
extern crate futures;
extern crate tokio_codec;
extern crate tokio_core;
extern crate tokio_io;
#[macro_use]
extern crate log;
extern crate log4rs;

mod coder;

use coder::{Tagged, TaggedDecoder, TaggedEncoder};
use futures::future::{self, Either, FutureResult, Loop};
use futures::unsync::mpsc::{channel, Sender};
use futures::unsync::oneshot;
use futures::{Future, Sink, Stream};
use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::rc::Rc;
use tokio_codec::{Decoder, Encoder, FramedRead, FramedWrite};
use tokio_core::reactor::Handle;
use tokio_io::io::{ReadHalf, WriteHalf};
use tokio_io::{AsyncRead, AsyncWrite};

type SporkRead<T, D> = FramedRead<ReadHalf<T>, TaggedDecoder<D>>;
type SporkWrite<T, E> = FramedWrite<WriteHalf<T>, TaggedEncoder<E>>;

/// A managed connection to a socket or other read/write data.
pub struct Spork<T, D, E>
where
  T: AsyncRead + AsyncWrite + Sized,
  D: Decoder + 'static,
  E: Encoder + 'static
{
  name: String,
  handle: Handle,
  read: SporkRead<T, D>,
  write: SporkWrite<T, E>,
  client_role: bool
}

impl<T, D, E> Spork<T, D, E>
where
  T: AsyncRead + AsyncWrite + Sized + 'static,
  D: Decoder + 'static,
  D::Error: Into<io::Error>,
  E: Encoder + 'static,
  E::Error: Into<io::Error>
{
  /// Construct a new spork which processes data from: a name used for debugging purposes; a asynchronous
  /// core handle; the socket on which messages are received and sent along with the decoder and encoder for
  /// those messages; and a flag indicating if this spork is operating in a client role.
  ///
  /// There are two roles for sporks: the client role and the server role. By convention, the client role is
  /// assigned to the spork that is on the socket-opening side of the connection, and the server role is
  /// assigned to the socket-listening side. However, the most important consideration is that sporks on either
  /// side of a connection aren't assigned to the same role. These roles exist for number messaging stategies:
  /// they ensure that the two sides of a connection don't generate the same conversation IDs.
  pub fn new(name: String, handle: Handle, socket: T, decoder: D, encoder: E, client_role: bool) -> Spork<T, D, E> {
    let (read, write) = socket.split();
    let read = FramedRead::new(read, TaggedDecoder::new(decoder));
    let write = FramedWrite::new(write, TaggedEncoder::new(encoder));
    Spork { name, handle, read, write, client_role }
  }

  /// Process messages coming from the server connection. This method returns:
  ///
  /// - A chatter to start conversations from this side of the connection.
  /// - A stream which generates messages from the other side of the connection. Each such message is the start
  ///   of a new conversation.
  ///
  /// See the package documentation or tests for examples of how to use this function.
  pub fn process(self) -> (Chatter<D, E>, impl Stream<Item = Response<D, E>, Error = io::Error>) {
    let (name, handle, read, write) = (self.name, self.handle, self.read, self.write);
    let channels = Channels::new(if self.client_role { 0 } else { 1 });

    let (in_send, in_recv) = channel(2);
    let (write_in, write_ftr) = out_pump(write);
    let read_ftr = InPump::new(&name, read, in_send, channels.clone()).pump();
    let chatter = Chatter::new(write_in, channels);

    let chatter_clone = chatter.clone();
    let incoming_resp =
      in_recv.map(move |m| Response::new(chatter_clone.clone(), m)).map_err(|_| io_err("Incoming data dropped."));

    handle.spawn(combine_rw(name, read_ftr, write_ftr));
    (chatter, incoming_resp)
  }
}

enum DoneType<T, D, E>
where
  D: Decoder + 'static,
  E: Encoder + 'static
{
  Read(SporkRead<T, D>),
  Write(SporkWrite<T, E>)
}

fn combine_rw<T, D, E, R, W>(name: String, read_ftr: R, write_ftr: W) -> impl Future<Item = (), Error = ()>
where
  D: Decoder + 'static,
  E: Encoder + 'static,
  R: Future<Item = SporkRead<T, D>, Error = (io::Error, SporkRead<T, D>)>,
  W: Future<Item = SporkWrite<T, E>, Error = (io::Error, Option<SporkWrite<T, E>>)>
{
  // Remap the read/write futures to have the same types, so they can be `select`-ed.
  let read_ftr = read_ftr.map(DoneType::Read).map_err(|(e, read)| (e, Some(DoneType::Read(read))));
  let write_ftr = write_ftr.map(DoneType::Write).map_err(|(e, write)| (e, write.map(DoneType::Write)));

  // We can't allow the read to complete if write completes (or vice versa), since it's likely that it will
  // continue forever, never completing with its error.

  let rw_ftr = read_ftr.select(write_ftr).then(move |v| match v {
    Ok((_first, _second)) => {
      debug!("Pump for \"{}\" done successfully.", name);
      future::ok(())
    }
    Err(((e1, _first), _second)) => {
      debug!("Pump for \"{}\" done with error: {:?}.", name, e1);
      future::err(())
    }
  });

  rw_ftr
}

fn out_pump<T, E>(
  write: SporkWrite<T, E>
) -> (Sender<Tagged<E::Item>>, impl Future<Item = SporkWrite<T, E>, Error = (io::Error, Option<SporkWrite<T, E>>)>)
where
  T: AsyncRead + AsyncWrite + Sized + 'static,
  E: Encoder + 'static,
  E::Error: Into<io::Error>
{
  let (send, rcv) = channel(2);

  let pump_ftr = future::loop_fn((rcv, write), move |(rcv, write)| {
    rcv.into_future().then(move |v| match v {
      Err(_) => Either::B(future::err((io_err("dropped"), Some(write)))),
      Ok((oval, rcv)) => match oval {
        Some(val) => Either::A(write.send(val).map_err(|e| (e.into(), None)).map(|write| Loop::Continue((rcv, write)))),
        None => Either::B(future::ok(Loop::Break(write)))
      }
    })
  });

  (send, pump_ftr)
}

struct InPump<T, D>
where
  T: AsyncRead + 'static,
  D: Decoder + 'static,
  D::Error: Into<io::Error>
{
  name: String,
  read: SporkRead<T, D>,
  new_key: Sender<Tagged<D::Item>>,
  channels: Channels<D::Item>
}

impl<T, D> InPump<T, D>
where
  T: AsyncRead + 'static,
  D: Decoder + 'static,
  D::Error: Into<io::Error>
{
  fn new(
    name: &str, read: SporkRead<T, D>, new_key: Sender<Tagged<D::Item>>, channels: Channels<D::Item>
  ) -> InPump<T, D> {
    InPump { name: name.to_string(), read, new_key, channels }
  }

  /// Dispatch or otherwise react to a message received from a server.
  ///
  /// This sends all messages to a channel, where they are responded to appropriately.
  fn pump(self) -> impl Future<Item = SporkRead<T, D>, Error = (io::Error, SporkRead<T, D>)> {
    future::loop_fn(self, move |dspr| {
      dspr.into_future().and_then(move |(oreq, dspr)| match oreq {
        Some(val) => Either::A(dspr.handle_next(val).map(|(_, dspr)| Loop::Continue(dspr))),
        None => Either::B(future::ok(Loop::Break(dspr)))
      })
    })
    .map(|dspr| dspr.into_read())
  }

  /// Just like `Stream.into_future`, but for the pump.
  fn into_future(
    self
  ) -> impl Future<Item = (Option<Tagged<D::Item>>, InPump<T, D>), Error = (io::Error, SporkRead<T, D>)> {
    let (name, read, new_key, channels) = (self.name, self.read, self.new_key, self.channels);

    read.into_future().then(move |v| match v {
      Ok((v, read)) => future::ok((v, InPump::new(&name, read, new_key, channels))),
      Err((e, read)) => future::err((e.into(), read))
    })
  }

  /// Handles a single server message by acting directly (such as with `Data` or `Open`), or by routing it to
  /// the appropriate channel.
  fn handle_next(
    self, val: Tagged<D::Item>
  ) -> impl Future<Item = ((), InPump<T, D>), Error = (io::Error, SporkRead<T, D>)> {
    let (name, read, new_key, channels) = (self.name, self.read, self.new_key, self.channels);

    let tag = val.tag();
    let channel: Option<Channel<D::Item>> = channels.remove(tag);
    match channel {
      Some(channel) => Either::A(channel.send(val).then(move |r| match r {
        Ok(_) => future::ok(((), InPump::new(&name, read, new_key, channels))),
        Err(e) => future::err((io_err(e.description()), read))
      })),
      None => Either::B(new_key.send(val).then(move |r| match r {
        Ok(new_key) => future::ok(((), InPump::new(&name, read, new_key, channels))),
        Err(e) => future::err((io_err(e.description()), read))
      }))
    }
  }

  fn into_read(self) -> SporkRead<T, D> { self.read }
}

/// A simple message sender/receiver that can initiate conversations from this side of a connection.
pub struct Chatter<D, E>
where
  D: Decoder + 'static,
  E: Encoder + 'static
{
  write: Sender<Tagged<E::Item>>,
  channels: Channels<D::Item>
}

impl<D, E> Clone for Chatter<D, E>
where
  D: Decoder + 'static,
  E: Encoder + 'static
{
  fn clone(&self) -> Chatter<D, E> { Chatter { write: self.write.clone(), channels: self.channels.clone() } }
}

impl<D, E> Chatter<D, E>
where
  D: Decoder + 'static,
  E: Encoder + 'static
{
  fn new(write: Sender<Tagged<E::Item>>, channels: Channels<D::Item>) -> Chatter<D, E> { Chatter { write, channels } }

  /// Send a message to the server, without expecting a particular response.
  pub fn say(&self, msg: E::Item) -> impl Future<Item = (), Error = io::Error> {
    self.say_keyed(Tagged::new(self.channels.next_key(), msg))
  }

  /// Send a message to the server, and return a future that has the response message.
  pub fn ask(&self, msg: E::Item) -> impl Future<Item = Response<D, E>, Error = io::Error> {
    self.ask_keyed(Tagged::new(self.channels.next_key(), msg))
  }

  fn say_keyed(&self, msg: Tagged<E::Item>) -> impl Future<Item = (), Error = io::Error> {
    let write = self.write.clone();
    write.send(msg).map_err(|e| io_err(e.description())).map(move |_| ())
  }

  fn ask_keyed(&self, msg: Tagged<E::Item>) -> impl Future<Item = Response<D, E>, Error = io::Error> {
    let (sender, receiver) = oneshot::channel();
    let read = receiver.map_err(|e| io_err(&format!("Cancelled while asking: {}", e.description())));
    self.channels.insert(msg.tag(), sender);
    let self_clone = self.clone();
    self.say_keyed(msg).and_then(|_| read).map(move |m| Response::new(self_clone, m))
  }
}

/// A message from the other side of a connection, in response to a `Chatter::ask` or `Response::ask` query from
/// this side. The response can be used to continue sending messages, if required.
pub struct Response<D, E>
where
  D: Decoder + 'static,
  E: Encoder + 'static
{
  chatter: Chatter<D, E>,
  message: Tagged<D::Item>
}

impl<D, E> Response<D, E>
where
  D: Decoder + 'static,
  E: Encoder + 'static
{
  fn new(chatter: Chatter<D, E>, message: Tagged<D::Item>) -> Response<D, E> { Response { chatter, message } }

  pub fn message(&self) -> &D::Item { &self.message.message() }

  /// Send a message to the server, without expecting a particular response.
  pub fn say(&self, msg: E::Item) -> impl Future<Item = (), Error = io::Error> {
    self.chatter.say_keyed(Tagged::new(self.message.tag(), msg))
  }

  /// Send a message to the server, and return a future that has the response message.
  pub fn ask(&self, msg: E::Item) -> impl Future<Item = Response<D, E>, Error = io::Error> {
    self.chatter.ask_keyed(Tagged::new(self.message.tag(), msg))
  }
}

struct Channels<T> {
  next_key: Rc<RefCell<u32>>,
  channels: Rc<RefCell<HashMap<u32, oneshot::Sender<Tagged<T>>>>>
}

impl<T> Clone for Channels<T> {
  fn clone(&self) -> Channels<T> { Channels { next_key: self.next_key.clone(), channels: self.channels.clone() } }
}

impl<T> Channels<T>
where
  T: 'static
{
  fn new(next_key: u32) -> Channels<T> {
    let channels = Rc::new(RefCell::new(HashMap::new()));
    let next_key = Rc::new(RefCell::new(next_key));
    Channels { next_key, channels }
  }

  fn next_key(&self) -> u32 {
    let key = *self.next_key.borrow();
    *self.next_key.borrow_mut() += 2;
    key
  }

  fn remove(&self, key: u32) -> Option<Channel<T>> { self.channels.borrow_mut().remove(&key).map(Channel::new) }

  fn insert(&self, key: u32, sender: oneshot::Sender<Tagged<T>>) { self.channels.borrow_mut().insert(key, sender); }
}

struct Channel<T> {
  sender: oneshot::Sender<Tagged<T>>
}

impl<T> Channel<T> {
  fn new(sender: oneshot::Sender<Tagged<T>>) -> Channel<T> { Channel { sender } }

  fn send(self, msg: Tagged<T>) -> FutureResult<(), io::Error> {
    future::result(self.sender.send(msg).map_err(|_| io_err("Couldn't send.")))
  }
}

fn io_err(desc: &str) -> io::Error { io::Error::new(io::ErrorKind::Other, desc) }

/// Unit tests.
#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_io_err() {
    use std::error::Error;
    let err = io_err("test");
    assert_eq!(err.description(), "test");
  }
}
