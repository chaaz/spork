//! A communication protocol that directs messages to ongoing registered conversations. Using a spork lets you
//! run multiple conversions with the other side of a connection simultaneously, and you can write them all
//! using async futures-style code.
//!
//! ```
//! # use futures::compat::AsyncRead01CompatExt;
//! # use futures::executor::LocalPool;
//! # use futures::future::*;
//! # use futures::future;
//! # use futures::io::{ReadHalf, WriteHalf, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
//! # use futures::sink::*;
//! # use futures::stream::*;
//! # use futures::task::SpawnExt;
//! # use mock_io::Builder;
//! # use spork::*;
//! # use spork::message::*;
//! # use std::io;
//! # use tokio::codec::*;
//! # fn io_err(desc: &str) -> io::Error { io::Error::new(io::ErrorKind::Other, desc) }
//! # fn handle_protocol_2(resp: Response<Dec, Enc>) -> impl Future<Output = Result<bool, io::Error>> {
//! #   future::ok(true)
//! # }
//! # fn main() {
//! # let encoder = Enc::new();
//! # let decoder = Dec::new();
//! # let mut pool = LocalPool::new();
//! # let socket = Builder::new()
//! #   .write(b"\x00\x00\x00\x00\x00\x00\x00\x09Sending 1")
//! #   .read(b"\x00\x00\x00\x00\x00\x00\x00\x05Got 1")
//! #   .read(b"\x00\x00\x00\x01\x00\x00\x00\x0aProtocol_1")
//! #   .write(b"\x00\x00\x00\x00\x00\x00\x00\x09Sending 2")
//! #   .read(b"\x00\x00\x00\x00\x00\x00\x00\x05Got 2")
//! #   .build()
//! #   .compat();
//! let spork = Spork::new("demo".into(), socket, decoder, encoder, true);
//! let (mut outgoing_msgs, incoming_msgs, driver, _) = spork.process();
//! pool.spawner().spawn(driver.unwrap_or_else(|e| println!("Error: {:?}", e))).unwrap();
//!
//! let outgoing = async move {
//!   let response1 = outgoing_msgs.ask("Sending 1").await?;
//!   if response1.message().text() != "Got 1" {
//!     return Err(io_err("Bad response 1."));
//!   }
//!   let response2 = response1.ask("Sending 2").await?;
//!   if response2.message().text() != "Got 2" {
//!     return Err(io_err("Bad response 2."));
//!   }
//!   Ok(println!("Done with protocol."))
//! };
//!
//! let incoming = incoming_msgs.try_for_each(|response| {
//!   match response.message().text() {
//!     "Protocol_1" => Either::Left(future::ok(true)),
//!     "Protocol_2" => Either::Right(handle_protocol_2(response)),
//!     _ => Either::Left(future::err(io_err("Unknown protocol.")))
//!   }
//!   .map_ok(|v| println!("Judgement: {}", v))
//! });
//! # pool.run_until(try_join(outgoing, incoming)).unwrap();
//! # }
//! ```

mod coder;
pub mod message;

use crate::coder::{Tagged, TaggedDecoder, TaggedEncoder};
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::channel::oneshot;
use futures::channel::oneshot::{Receiver as OneReceiver, Sender as OneSender};
use futures::compat::{Compat, Compat01As03, Compat01As03Sink, Sink01CompatExt, Stream01CompatExt};
use futures::future::{self, try_select, Either, Future, FutureExt, Ready, TryFutureExt};
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use futures::sink::SinkExt;
use futures::stream::{Stream, StreamExt};
use log::debug;
use std::boxed::Box;
use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::sync::{Arc, Mutex};
use tokio::codec::{Decoder, Encoder, FramedRead, FramedWrite};

type SporkRead<T, D> = Compat01As03<FramedRead<Compat<ReadHalf<T>>, TaggedDecoder<D>>>;
type SporkWrite<T, E> =
  Compat01As03Sink<FramedWrite<Compat<WriteHalf<T>>, TaggedEncoder<E>>, Tagged<<E as Encoder>::Item>>;
type Er = std::io::Error;

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
  D::Error: Into<Er>,
  E: Encoder + 'static,
  E::Error: Into<Er>
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
    let read = FramedRead::new(read.compat(), TaggedDecoder::new(decoder)).compat();
    let write = FramedWrite::new(write.compat_write(), TaggedEncoder::new(encoder)).sink_compat();
    Spork { name, read, write, client_role }
  }

  /// Process messages coming from the server connection. This method returns:
  ///
  /// - A chatter to start conversations from this side of the connection.
  /// - A stream which generates messages from the other side of the connection. Each such message is the start
  ///   of a new conversation.
  /// - A "driver" future which manages the internal channels to/from the socket.
  /// - An "interrupter", which can be used to interrupt the driver by sending it ().
  ///
  /// # Examples
  ///
  /// The driver future must be constantly `poll`ed to drive the socket communication. If you're not doing
  /// anything special at the end of the communication, it's easiest to just spawn a new task for it:
  ///
  /// ```rust
  /// # use futures::compat::*;
  /// # use futures::executor::LocalPool;
  /// # use futures::future::*;
  /// # use futures::future;
  /// # use futures::io::{ReadHalf, WriteHalf, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
  /// # use futures::sink::*;
  /// # use futures::stream::*;
  /// # use futures::task::SpawnExt;
  /// # use log::debug;
  /// # use mock_io::Builder;
  /// # use spork::*;
  /// # use spork::message::*;
  /// # use std::io;
  /// # use tokio::codec::{Decoder, Encoder};
  /// # fn do_spawn<T, D, E>(spork: Spork<T, D, E>)
  /// # where
  /// #   T: AsyncRead + AsyncWrite + Sized + std::marker::Send + 'static,
  /// #   D: Decoder + std::marker::Send + 'static,
  /// #   D::Error: Into<io::Error> + std::marker::Send,
  /// #   D::Item: std::marker::Send,
  /// #   E: Encoder + std::marker::Send + 'static,
  /// #   E::Error: Into<io::Error> + std::marker::Send,
  /// #   E::Item: std::marker::Send
  /// # {
  /// let mut pool = LocalPool::new();
  /// let (chat, msgs, driver, _) = spork.process();
  /// pool.spawner().spawn(driver.unwrap_or_else(|e| println!("Error: {:?}", e))).unwrap();
  /// # }
  /// # fn main() {}
  /// ```
  ///
  /// See the package documentation or tests for examples of how to use this function.
  pub fn process(
    self
  ) -> (
    Chatter<D, E>,
    impl Stream<Item = Result<Response<D, E>, Er>>,
    impl Future<Output = Result<(), Er>>,
    OneSender<()>
  ) {
    let (name, read, write) = (self.name, self.read, self.write);
    let channels = Channels::new(if self.client_role { 0 } else { 1 });

    let (in_send, in_recv) = channel(2);
    let (write_in, write_ftr) = out_pump(write);
    let read_ftr = in_pump(read, in_send, channels.clone());
    let chatter = Chatter::new(write_in, channels);
    let (intr_send, intr_recv) = futures::channel::oneshot::channel();

    let chatter_clone = chatter.clone();
    let incoming_resp = in_recv.map(move |m| Ok(Response::new(chatter_clone.clone(), m)));

    (chatter, incoming_resp, combine_rw(name, Box::pin(read_ftr), Box::pin(write_ftr), intr_recv), intr_send)
  }
}

async fn combine_rw<R, W>(name: String, read_ftr: R, write_ftr: W, intr: OneReceiver<()>) -> Result<(), Er>
where
  R: Future<Output = Result<(), Er>> + Unpin,
  W: Future<Output = Result<(), Er>> + Unpin
{
  // We can't allow the read to complete if write completes (or vice versa), since it's likely that it will
  // continue forever, never completing with its error.

  let double = try_select(read_ftr, write_ftr)
    .map_ok(|_| {
      debug!("Pump for \"{}\" done successfully.", name);
      ()
    })
    .map_err(|e| {
      let e = e.factor_first().0;
      debug!("Pump for \"{}\" done with error: {:?}.", name, e);
      e
    });

  match try_select(intr, double).await {
    Ok(_) => Ok(()),
    Err(Either::Left((_canceled, double))) => {
      // If our user just dropped the interrupter, they clearly don't care about interrupting.
      double.await
    }
    Err(Either::Right((e, _intr))) => Err(e)
  }
}

fn out_pump<T, E>(write: SporkWrite<T, E>) -> (Sender<Tagged<E::Item>>, impl Future<Output = Result<(), Er>>)
where
  T: AsyncRead + AsyncWrite + Sized + 'static,
  E: Encoder + 'static,
  E::Error: Into<Er>
{
  let (send, rcv) = channel(2);
  (send, out_loop(write, rcv))
}

async fn out_loop<T, E>(mut write: SporkWrite<T, E>, mut rcv: Receiver<Tagged<E::Item>>) -> Result<(), Er>
where
  T: AsyncRead + AsyncWrite + Sized + 'static,
  E: Encoder + 'static,
  E::Error: Into<Er>
{
  while let Some(val) = rcv.next().await {
    write.send(val).await.map_err(|e| e.into())?
  }
  Ok(())
}

/// Listen on our reader, and send each received item to the appropriate channel.
async fn in_pump<T, D>(
  mut read: SporkRead<T, D>, mut new_key: Sender<Tagged<D::Item>>, mut channels: Channels<D::Item>
) -> Result<(), Er>
where
  T: AsyncRead + 'static,
  D: Decoder + 'static,
  D::Error: Into<Er>
{
  while let Some(val) = read.next().await {
    handle_next::<D>(&mut new_key, &mut channels, val.map_err(|e| e.into())?).await?
  }

  Ok(())
}

async fn handle_next<D>(
  new_key: &mut Sender<Tagged<D::Item>>, channels: &mut Channels<D::Item>, val: Tagged<D::Item>
) -> Result<(), Er>
where
  D: Decoder + 'static,
  D::Error: Into<Er>
{
  let tag = val.tag();
  let channel: Option<Channel<D::Item>> = channels.remove(tag);

  match channel {
    Some(channel) => channel.send(val).await.map_err(|e| io_err(e.description())),
    None => new_key.send(val).await.map_err(|e| io_err(e.description()))
  }
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
  pub fn say<M: Into<E::Item>>(&mut self, message: M) -> impl Future<Output = Result<(), Er>> + '_ {
    let next_key = self.channels.next_key();
    self.say_tagged(Tagged::new(next_key, message.into()))
  }

  /// Send a message to the server, and return a future that has the response message.
  pub fn ask<M: Into<E::Item>>(&mut self, message: M) -> impl Future<Output = Result<Response<D, E>, Er>> + '_ {
    let next_key = self.channels.next_key();
    self.ask_tagged(Tagged::new(next_key, message.into()))
  }

  /// Send a message to the server, without expecting a particular response.
  pub fn into_say<M: Into<E::Item>>(self, message: M) -> impl Future<Output = Result<(), Er>> {
    let next_key = self.channels.next_key();
    self.into_say_tagged(Tagged::new(next_key, message.into()))
  }

  /// Send a message to the server, and return a future that has the response message.
  pub fn into_ask<M: Into<E::Item>>(self, message: M) -> impl Future<Output = Result<Response<D, E>, Er>> {
    let next_key = self.channels.next_key();
    self.into_ask_tagged(Tagged::new(next_key, message.into()))
  }

  fn ask_tagged(&mut self, message: Tagged<E::Item>) -> impl Future<Output = Result<Response<D, E>, Er>> + '_ {
    let (sender, receiver) = oneshot::channel();
    let read = receiver.map(|v| v.map_err(|e| io_err(&format!("Cancelled while asking: {}", e.description()))));
    self.channels.insert(message.tag(), sender);
    let self_clone = self.clone();
    self.say_tagged(message).and_then(|_| read).map(move |v| v.map(move |m| Response::new(self_clone, m)))
  }

  async fn say_tagged(&mut self, message: Tagged<E::Item>) -> Result<(), Er> {
    self.write.send(message).await.map_err(|e| io_err(e.description()))
  }

  fn into_ask_tagged(self, message: Tagged<E::Item>) -> impl Future<Output = Result<Response<D, E>, Er>> {
    let (sender, receiver) = oneshot::channel();
    let read = receiver.map(|v| v.map_err(|e| io_err(&format!("Cancelled while asking: {}", e.description()))));
    self.channels.insert(message.tag(), sender);
    let self_clone = self.clone();
    self.into_say_tagged(message).and_then(|_| read).map(move |v| v.map(move |m| Response::new(self_clone, m)))
  }

  fn into_say_tagged(mut self, message: Tagged<E::Item>) -> impl Future<Output = Result<(), Er>> {
    async move { self.write.send(message).await.map_err(|e| io_err(e.description())) }
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
  pub fn say<M: Into<E::Item>>(self, message: M) -> impl Future<Output = Result<(), Er>> {
    let tag = self.message.tag();
    self.chatter.into_say_tagged(Tagged::new(tag, message.into()))
  }

  /// Send a message to the server, and return a future that has the response message.
  pub fn ask<M: Into<E::Item>>(self, message: M) -> impl Future<Output = Result<Response<D, E>, Er>> {
    let tag = self.message.tag();
    self.chatter.into_ask_tagged(Tagged::new(tag, message.into()))
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

  /// Get the chatter that originated this response, so that further conversations can be had.
  pub fn chatter(&self) -> Chatter<D, E> { self.chatter.clone() }

  /// Send a message to the server, without expecting a particular response.
  pub fn say<M: Into<E::Item>>(self, message: M) -> impl Future<Output = Result<(), Er>> {
    let tag = self.tag;
    self.chatter.into_say_tagged(Tagged::new(tag, message.into()))
  }

  /// Send a message to the server, and return a future that has the response message.
  pub fn ask<M: Into<E::Item>>(self, message: M) -> impl Future<Output = Result<Response<D, E>, Er>> {
    let tag = self.tag;
    self.chatter.into_ask_tagged(Tagged::new(tag, message.into()))
  }

  /// Create a new response coming back from the server, based on this responder. See `Response::split`.
  pub fn join(self, message: D::Item) -> Response<D, E> { Response::new(self.chatter, Tagged::new(self.tag, message)) }
}

struct Channels<T> {
  next_key: Arc<Mutex<u32>>,
  channels: Arc<Mutex<HashMap<u32, oneshot::Sender<Tagged<T>>>>>
}

impl<T> Clone for Channels<T> {
  fn clone(&self) -> Channels<T> { Channels { next_key: self.next_key.clone(), channels: self.channels.clone() } }
}

impl<T> Channels<T>
where
  T: 'static
{
  fn new(next_key: u32) -> Channels<T> {
    let channels = Arc::new(Mutex::new(HashMap::new()));
    let next_key = Arc::new(Mutex::new(next_key));
    Channels { next_key, channels }
  }

  fn next_key(&self) -> u32 {
    let key = *self.next_key.lock().unwrap();
    *self.next_key.lock().unwrap() += 2;
    key
  }

  fn remove(&self, key: u32) -> Option<Channel<T>> { self.channels.lock().unwrap().remove(&key).map(Channel::new) }

  fn insert(&self, key: u32, sender: oneshot::Sender<Tagged<T>>) { self.channels.lock().unwrap().insert(key, sender); }
}

struct Channel<T> {
  sender: oneshot::Sender<Tagged<T>>
}

impl<T> Channel<T> {
  fn new(sender: oneshot::Sender<Tagged<T>>) -> Channel<T> { Channel { sender } }

  fn send(self, message: Tagged<T>) -> Ready<Result<(), Er>> {
    future::ready(self.sender.send(message).map_err(|_| io_err("Couldn't send.")))
  }
}

fn io_err(desc: &str) -> Er { Er::new(io::ErrorKind::Other, desc) }

#[cfg(test)]
mod tests {
  use super::*;
  use futures::compat::AsyncRead01CompatExt;
  use futures::executor::LocalPool;
  use futures::future::Either;
  use futures::stream::TryStreamExt;
  use futures::task::SpawnExt;
  use message::*;
  use mock_io::Builder;

  #[test]
  fn test_io_err() {
    use std::error::Error;
    let err = io_err("Test error.");
    assert_eq!(err.description(), "Test error.");
  }

  #[test]
  fn test_chatter_say() {
    let mut chatter = setup_chatter();
    let channels = chatter.channels.channels.clone();
    drop(chatter.say("Sending without expecting response."));
    assert_eq!(0, channels.lock().unwrap().len());
  }

  #[test]
  fn test_chatter_ask() {
    let mut chatter = setup_chatter();
    let channels = chatter.channels.channels.clone();
    drop(chatter.ask("What is the response?"));
    assert_eq!(1, channels.lock().unwrap().len());
  }

  #[test]
  fn test_outgoing() {
    let mut pool = LocalPool::new();
    let mocket = Builder::new()
      .write(b"\x00\x00\x00\x00\x00\x00\x00\x09Sending 1")
      .read(b"\x00\x00\x00\x00\x00\x00\x00\x05Got 1")
      .build()
      .compat();
    let verification = Arc::new(Mutex::new(Vec::new()));

    let client = Spork::new("test".into(), mocket, Dec::new(), Enc::new(), true);
    let (mut client_chat, _, driver, _) = client.process();
    pool.spawner().spawn(driver.unwrap_or_else(|e| println!("Error: {:?}", e))).unwrap();

    let channels = client_chat.channels.channels.clone();
    let client_comm = client_chat
      .ask("Sending 1")
      .and_then(|response| {
        verification.lock().unwrap().push(1);
        match response.message().text() {
          "Got 1" => {
            verification.lock().unwrap().push(2);
            future::ok(response)
          }
          _ => future::err(io_err("Bad from client."))
        }
      })
      .map(|r| r.map(|_| verification.lock().unwrap().push(3)).map_err(|e| println!("Got server error: {:?}.", e)));

    pool.run_until(client_comm).unwrap();
    let verification = verification.lock().unwrap();
    assert_eq!(verification.as_slice(), &[1, 2, 3]);
    assert_eq!(0, channels.lock().unwrap().len());
  }

  #[test]
  fn test_outgoing_async_keyword() {
    let mut pool = LocalPool::new();
    let mocket = Builder::new()
      .write(b"\x00\x00\x00\x00\x00\x00\x00\x09Sending 1")
      .read(b"\x00\x00\x00\x00\x00\x00\x00\x05Got 1")
      .build()
      .compat();
    let verification = Arc::new(Mutex::new(Vec::new()));

    let client = Spork::new("test".into(), mocket, Dec::new(), Enc::new(), true);
    let (mut client_chat, _, driver, _) = client.process();
    pool.spawner().spawn(driver.unwrap_or_else(|e| println!("Error: {:?}", e))).unwrap();

    let channels = client_chat.channels.channels.clone();
    let client_comm = async {
      let response = client_chat.ask("Sending 1").await?;
      verification.lock().unwrap().push(1);
      if response.message().text() != "Got 1" {
        return Err(io_err("Bad from client."));
      } else {
        verification.lock().unwrap().push(2);
      }
      Ok(verification.lock().unwrap().push(3))
    };

    pool.run_until(client_comm).unwrap();
    let verification = verification.lock().unwrap();
    assert_eq!(verification.as_slice(), &[1, 2, 3]);
    assert_eq!(0, channels.lock().unwrap().len());
  }

  #[test]
  fn test_incoming() {
    let mut pool = LocalPool::new();
    let mocket = Builder::new()
      .read(b"\x00\x00\x00\x00\x00\x00\x00\x09Sending 1")
      .write(b"\x00\x00\x00\x00\x00\x00\x00\x05Got 1")
      .build()
      .compat();
    let verification = Arc::new(Mutex::new(Vec::new()));

    let server = Spork::new("test".into(), mocket, Dec::new(), Enc::new(), false);
    let (server_chat, server_comm, driver, _) = server.process();
    pool.spawner().spawn(driver.unwrap_or_else(|e| println!("Error: {:?}", e))).unwrap();

    let server_comm = server_comm
      .try_for_each(|msg| {
        verification.lock().unwrap().push(1);
        match msg.message().text() {
          "Sending 1" => {
            verification.lock().unwrap().push(2);
            Either::Left(msg.say("Got 1"))
          }
          _ => Either::Right(future::err(io_err("Bad msg from server.")))
        }
      })
      .map_ok(|_| verification.lock().unwrap().push(3))
      .map_err(|e| println!("Got server error: {:?}.", e));

    pool.run_until(server_comm).unwrap();
    let verification = verification.lock().unwrap();
    assert_eq!(verification.as_slice(), &[1, 2, 3]);
    assert_eq!(0, server_chat.channels.channels.lock().unwrap().len());
  }

  fn setup_chatter() -> Chatter<Dec, Enc> {
    let (write, _) = channel(0);
    let channels = Channels::new(0);
    Chatter::new(write, channels)
  }
}
