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
//! # extern crate mock_io;
//! # use futures::{Future, Stream};
//! # use futures::future::{self, Either};
//! # use spork::*;
//! # use spork::message::*;
//! # use std::io;
//! # use tokio_core::reactor::Core;
//! # use mock_io::Builder;
//! # fn io_err(desc: &str) -> io::Error { io::Error::new(io::ErrorKind::Other, desc) }
//! # fn judge_protocol_2(resp: Response<Dec, Enc>) -> impl Future<Item = bool, Error = io::Error> {
//! #   future::ok(true)
//! # }
//! # fn main() {
//! # let encoder = Enc::new();
//! # let decoder = Dec::new();
//! # let mut core = Core::new().unwrap();
//! # let socket = Builder::new()
//! #   .write(b"\x00\x00\x00\x00\x00\x00\x00\x09Sending 1")
//! #   .read(b"\x00\x00\x00\x00\x00\x00\x00\x05Got 1")
//! #   .read(b"\x00\x00\x00\x01\x00\x00\x00\x0aProtocol_1")
//! #   .write(b"\x00\x00\x00\x00\x00\x00\x00\x09Sending 2")
//! #   .read(b"\x00\x00\x00\x00\x00\x00\x00\x05Got 2")
//! #   .build();
//! let spork = Spork::new("demo".into(), socket, decoder, encoder, true);
//! let (chat, incoming_msgs, driver) = spork.process();
//! # let handle = core.handle();
//! handle.spawn(driver.map_err(|_| ()));
//!
//! let outgoing_conversation = chat
//!   .ask(Message::new("Sending 1"))
//!   .and_then(|response| match response.message().text() {
//!     "Got 1" => future::ok(response),
//!     _ => future::err(io_err("Bad response 1."))
//!   })
//!   .and_then(|response| response.ask(Message::new("Sending 2")))
//!   .and_then(|response| match response.message().text() {
//!     "Got 2" => future::ok(()),
//!     _ => future::err(io_err(&format!("Bad response 2: {}.", response.message().text())))
//!   })
//!   .map(|_| println!("Done with protocol."))
//!   .map_err(|e| println!("Got error: {:?}.", e));
//!
//! let incoming_conversations = incoming_msgs
//!   .for_each(|response| {
//!     match response.message().text() {
//!       "Protocol_1" => Either::A(future::ok(true)),
//!       "Protocol_2" => Either::B(judge_protocol_2(response)),
//!       _ => Either::A(future::err(io_err(&format!("Unknown protocol {}.", response.message().text()))))
//!     }
//!     .map(|v| println!("Judgement: {}", v))
//!   })
//!   .map(|_| println!("Done with incoming."))
//!   .map_err(|e| println!("Got error: {:?}", e));
//! # core.run(outgoing_conversation.join(incoming_conversations).map(|_| ()).map_err(|_| ())).unwrap();
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
#[cfg(test)]
extern crate mock_io;

mod coder;
pub mod message;

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
  /// Construct a new spork which processes data. It's built from a name used for debugging purposes; the socket
  /// on which messages are received and sent; the decoder and encoder for those messages; and a flag indicating
  /// if this spork is operating in the client role.
  ///
  /// There are two roles for sporks: the client role and the server role. By convention, the client role is
  /// assigned to the spork that is on the socket-opening side of the connection, and the server role is
  /// assigned to the socket-listening side. However, the most important consideration is that sporks on either
  /// side of a connection aren't assigned to the same role. These roles exist for number messaging stategies:
  /// they ensure that the two sides of a connection don't generate the same conversation IDs.
  pub fn new(name: String, socket: T, decoder: D, encoder: E, client_role: bool) -> Spork<T, D, E> {
    let (read, write) = socket.split();
    let read = FramedRead::new(read, TaggedDecoder::new(decoder));
    let write = FramedWrite::new(write, TaggedEncoder::new(encoder));
    Spork { name, read, write, client_role }
  }

  /// Process messages coming from the server connection. This method returns:
  ///
  /// - A chatter to start conversations from this side of the connection.
  /// - A stream which generates messages from the other side of the connection. Each such message is the start
  ///   of a new conversation.
  /// - A "driver" future which manages the internal channels to/from the socket.
  ///
  /// # Examples
  ///
  /// The driver future must be constantly `poll`ed to drive the socket communication. If you're not doing
  /// anything special at the end of the communication, it's easiest to just spawn a new task for it:
  ///
  /// ```rust
  /// # extern crate futures;
  /// # extern crate spork;
  /// # extern crate tokio_codec;
  /// # extern crate tokio_core;
  /// # extern crate tokio_io;
  /// # use futures::future::Future;
  /// # use spork::*;
  /// # use std::io;
  /// # use tokio_codec::{Decoder, Encoder};
  /// # use tokio_core::reactor::*;
  /// # use tokio_io::{AsyncRead, AsyncWrite};
  /// # fn do_spawn<T, D, E>(spork: Spork<T, D, E>, handle: Handle)
  /// # where
  /// #   T: AsyncRead + AsyncWrite + Sized + 'static,
  /// #   D: Decoder + 'static,
  /// #   D::Error: Into<io::Error>,
  /// #   E: Encoder + 'static,
  /// #   E::Error: Into<io::Error>
  /// # {
  /// let (chat, msgs, driver) = spork.process();
  /// handle.spawn(driver.map_err(|e| eprintln!("{:?}", e)));
  /// # }
  /// # fn main() {}
  /// ```
  ///
  /// See the package documentation or tests for examples of how to use this function.
  pub fn process(
    self
  ) -> (Chatter<D, E>, impl Stream<Item = Response<D, E>, Error = io::Error>, impl Future<Item = (), Error = io::Error>)
  {
    let (name, read, write) = (self.name, self.read, self.write);
    let channels = Channels::new(if self.client_role { 0 } else { 1 });

    let (in_send, in_recv) = channel(2);
    let (write_in, write_ftr) = out_pump(write);
    let read_ftr = InPump::new(&name, read, in_send, channels.clone()).pump();
    let chatter = Chatter::new(write_in, channels);

    let chatter_clone = chatter.clone();
    let incoming_resp =
      in_recv.map(move |m| Response::new(chatter_clone.clone(), m)).map_err(|_| io_err("Incoming data dropped."));

    (chatter, incoming_resp, combine_rw(name, read_ftr, write_ftr))
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

fn combine_rw<T, D, E, R, W>(name: String, read_ftr: R, write_ftr: W) -> impl Future<Item = (), Error = io::Error>
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

  read_ftr.select(write_ftr).then(move |v| match v {
    Ok((_first, _second)) => {
      debug!("Pump for \"{}\" done successfully.", name);
      future::ok(())
    }
    Err(((e, _first), _second)) => {
      debug!("Pump for \"{}\" done with error: {:?}.", name, e);
      future::err(e)
    }
  })
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

  /// Listen on our reader, and send each received item to the appropriate channel.
  fn pump(self) -> impl Future<Item = SporkRead<T, D>, Error = (io::Error, SporkRead<T, D>)> {
    future::loop_fn(self, move |dspr| {
      dspr.into_future().and_then(move |(oreq, dspr)| match oreq {
        Some(val) => Either::A(dspr.handle_next(val).map(|(_, dspr)| Loop::Continue(dspr))),
        None => Either::B(future::ok(Loop::Break(dspr)))
      })
    })
    .map(|dspr| dspr.into_read())
  }

  fn into_future(
    self
  ) -> impl Future<Item = (Option<Tagged<D::Item>>, InPump<T, D>), Error = (io::Error, SporkRead<T, D>)> {
    let (name, read, new_key, channels) = (self.name, self.read, self.new_key, self.channels);

    read.into_future().then(move |v| match v {
      Ok((v, read)) => future::ok((v, InPump::new(&name, read, new_key, channels))),
      Err((e, read)) => future::err((e.into(), read))
    })
  }

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
  pub fn say(&self, message: E::Item) -> impl Future<Item = (), Error = io::Error> {
    self.say_tagged(Tagged::new(self.channels.next_key(), message))
  }

  /// Send a message to the server, and return a future that has the response message.
  pub fn ask(&self, message: E::Item) -> impl Future<Item = Response<D, E>, Error = io::Error> {
    self.ask_tagged(Tagged::new(self.channels.next_key(), message))
  }

  fn ask_tagged(&self, message: Tagged<E::Item>) -> impl Future<Item = Response<D, E>, Error = io::Error> {
    let (sender, receiver) = oneshot::channel();
    let read = receiver.map_err(|e| io_err(&format!("Cancelled while asking: {}", e.description())));
    self.channels.insert(message.tag(), sender);
    let self_clone = self.clone();
    self.say_tagged(message).and_then(|_| read).map(move |m| Response::new(self_clone, m))
  }

  fn say_tagged(&self, message: Tagged<E::Item>) -> impl Future<Item = (), Error = io::Error> {
    let write = self.write.clone();
    write.send(message).map_err(|e| io_err(e.description())).map(move |_| ())
  }
}

/// A message from the other side of a connection, in response to a `Chatter::ask` or `Response::ask` query from
/// this side. The response can be used to continue sending messages.
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

  pub fn split(self) -> (Responder<D, E>, D::Item) {
    (Responder::new(self.chatter, self.message.tag()), self.message.into_message())
  }

  /// Send a message to the server, without expecting a particular response.
  pub fn say(&self, message: E::Item) -> impl Future<Item = (), Error = io::Error> {
    self.chatter.say_tagged(Tagged::new(self.message.tag(), message))
  }

  /// Send a message to the server, and return a future that has the response message.
  pub fn ask(&self, message: E::Item) -> impl Future<Item = Response<D, E>, Error = io::Error> {
    self.chatter.ask_tagged(Tagged::new(self.message.tag(), message))
  }
}

/// The responder component of a response to a `Chatter::ask` or `Responder::ask` query. The responder doesn't
/// contain the answering message, but can be used to continue sending messages.
pub struct Responder<D, E>
where
  D: Decoder + 'static,
  E: Encoder + 'static
{
  chatter: Chatter<D, E>,
  tag: u32
}

impl<D, E> Clone for Responder<D, E>
where
  D: Decoder + 'static,
  E: Encoder + 'static
{
  fn clone(&self) -> Responder<D, E> { Responder { chatter: self.chatter.clone(), tag: self.tag } }
}

impl<D, E> Responder<D, E>
where
  D: Decoder + 'static,
  E: Encoder + 'static
{
  fn new(chatter: Chatter<D, E>, tag: u32) -> Responder<D, E> { Responder { chatter, tag } }

  /// Send a message to the server, without expecting a particular response.
  pub fn say(&self, message: E::Item) -> impl Future<Item = (), Error = io::Error> {
    self.chatter.say_tagged(Tagged::new(self.tag, message))
  }

  /// Send a message to the server, and return a future that has the response message.
  pub fn ask(&self, message: E::Item) -> impl Future<Item = Response<D, E>, Error = io::Error> {
    self.chatter.ask_tagged(Tagged::new(self.tag, message))
  }

  /// Create a new response coming back from the server, based on this responder. See `Response::split`.
  pub fn join(self, message: D::Item) -> Response<D, E> { Response::new(self.chatter, Tagged::new(self.tag, message)) }
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

  fn send(self, message: Tagged<T>) -> FutureResult<(), io::Error> {
    future::result(self.sender.send(message).map_err(|_| io_err("Couldn't send.")))
  }
}

fn io_err(desc: &str) -> io::Error { io::Error::new(io::ErrorKind::Other, desc) }

#[cfg(test)]
mod tests {
  use super::*;
  use message::*;
  use mock_io::Builder;
  use tokio_core::reactor::Core;

  #[test]
  fn test_io_err() {
    use std::error::Error;
    let err = io_err("test");
    assert_eq!(err.description(), "test");
  }

  #[test]
  fn test_chatter_say() {
    let chatter = setup_chatter();
    drop(chatter.say(Message::new("this is great.")));
    assert_eq!(0, chatter.channels.channels.borrow().len());
  }

  #[test]
  fn test_chatter_ask() {
    let chatter = setup_chatter();
    drop(chatter.ask(Message::new("this is great.")));
    assert_eq!(1, chatter.channels.channels.borrow().len());
  }

  #[test]
  fn test_outgoing() {
    let mut core = Core::new().unwrap();
    let mocket = Builder::new()
      .write(b"\x00\x00\x00\x00\x00\x00\x00\x09Sending 1")
      .read(b"\x00\x00\x00\x00\x00\x00\x00\x05Got 1")
      .build();
    let verification = Rc::new(RefCell::new(Vec::new()));

    let client = Spork::new("test".into(), mocket, Dec::new(), Enc::new(), true);
    let (client_chat, _, driver) = client.process();
    core.handle().spawn(driver.map_err(|_| ()));

    let client_comm = client_chat
      .ask(Message::new("Sending 1"))
      .and_then(|response| {
        verification.borrow_mut().push(1);
        match response.message().text() {
          "Got 1" => {
            verification.borrow_mut().push(2);
            future::ok(response)
          }
          _ => future::err(io_err("Bad from client."))
        }
      })
      .map(|_| verification.borrow_mut().push(3))
      .map_err(|e| println!("Got server error: {:?}.", e));

    core.run(client_comm.map(|_| ()).map_err(|_| ())).unwrap();
    let verification = verification.borrow();
    assert_eq!(verification.as_slice(), &[1, 2, 3]);
    assert_eq!(0, client_chat.channels.channels.borrow().len());
  }

  #[test]
  fn test_incoming() {
    let mut core = Core::new().unwrap();
    let mocket = Builder::new()
      .read(b"\x00\x00\x00\x00\x00\x00\x00\x08Server 1")
      .write(b"\x00\x00\x00\x00\x00\x00\x00\x04Ok 1")
      .build();
    let verification = Rc::new(RefCell::new(Vec::new()));

    let server = Spork::new("test".into(), mocket, Dec::new(), Enc::new(), false);
    let (server_chat, server_comm, driver) = server.process();
    core.handle().spawn(driver.map_err(|_| ()));

    let server_comm = server_comm
      .and_then(|msg| {
        verification.borrow_mut().push(1);
        match msg.message().text() {
          "Server 1" => {
            verification.borrow_mut().push(2);
            Either::A(msg.say(Message::new("Ok 1")))
          }
          _ => Either::B(future::err(io_err("Bad msg from server.")))
        }
      })
      .map(|_| verification.borrow_mut().push(3))
      .map_err(|e| println!("Got client error: {:?}.", e));

    core.run(server_comm.into_future().map(|_| ()).map_err(|_| ())).unwrap();
    let verification = verification.borrow();
    assert_eq!(verification.as_slice(), &[1, 2, 3]);
    assert_eq!(0, server_chat.channels.channels.borrow().len());
  }

  fn setup_chatter() -> Chatter<Dec, Enc> {
    let (write, _) = channel(0);
    let channels = Channels::new(0);
    Chatter::new(write, channels)
  }
}
