//! A communication protocol that directs messages to ongoing registered conversations. Using a spork lets you
//! run multiple conversions with the other side of a connection simultaneously, and you can write them all
//! using async futures-style code.
//!
//! ```
//! # use error_chain::bail;
//! # use tokio::runtime::Runtime;
//! # use futures::future::*;
//! # use futures::future;
//! # use futures::io::{ReadHalf, WriteHalf, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
//! # use futures::sink::*;
//! # use futures::stream::*;
//! # use futures::task::SpawnExt;
//! # use spork::*;
//! # use spork::errors::*;
//! # use spork::message::*;
//! # use std::io;
//! # use tokio_test::io::Builder;
//! # use tokio_util::codec::*;
//! # use tokio_util::compat::Tokio02AsyncReadCompatExt;
//! # fn handle_protocol_2(resp: Response<Message, Message>) -> impl Future<Output = Result<bool>> {
//! #   future::ok(true)
//! # }
//! # fn block_on<F: Future>(f: F) -> F::Output { Runtime::new().unwrap().block_on(f) }
//! # let encoder = Enc::new();
//! # let decoder = Dec::new();
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
//!
//! let outgoing = async move {
//!   let response1 = outgoing_msgs.ask("Sending 1").await?;
//!   if response1.message().text() != "Got 1" {
//!     bail!("Bad response 1.");
//!   }
//!   let response2 = response1.ask("Sending 2").await?;
//!   if response2.message().text() != "Got 2" {
//!     bail!("Bad response 2.");
//!   }
//!   Ok(println!("Done with protocol."))
//! };
//!
//! let incoming = incoming_msgs.try_for_each(|response| {
//!   match response.message().text() {
//!     "Protocol_1" => Either::Left(future::ok(true)),
//!     "Protocol_2" => Either::Right(handle_protocol_2(response)),
//!     _ => Either::Left(future::err(bad!("Unknown protocol.")))
//!   }
//!   .map_ok(|v| println!("Judgement: {}", v))
//! });
//! # block_on(try_join(try_join(outgoing, incoming), driver).map(|r| r.map(|_| ()))).unwrap();
//! ```

mod coder;
pub mod errors;
pub mod message;

use crate::coder::{Tagged, TaggedDecoder, TaggedEncoder};
use crate::errors::{Error, Result};
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::channel::oneshot;
use futures::future::{self, try_select, Either, Future, FutureExt, Ready, TryFutureExt};
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadHalf, WriteHalf};
use futures::sink::SinkExt;
use futures::stream::{Stream, StreamExt};
use log::debug;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::time::{timeout, Duration};
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, FuturesAsyncWriteCompatExt};

const ASK_TIMEOUT_SECS: u64 = 5;
type SporkRead<T, D> = FramedRead<Compat<ReadHalf<T>>, TaggedDecoder<D>>;
type SporkWrite<T, E, EI> = FramedWrite<Compat<WriteHalf<T>>, TaggedEncoder<E, EI>>;
type Er = Error;

/// A managed connection to a socket or other read/write data.
pub struct Spork<T, D, E, EI>
where
  T: AsyncRead + AsyncWrite + Sized,
  D: Decoder + 'static,
  E: Encoder<EI> + 'static + Send,
  EI: 'static + Send
{
  name: String,
  read: SporkRead<T, D>,
  write: SporkWrite<T, E, EI>,
  client_role: bool
}

impl<T, D, E, EI> Spork<T, D, E, EI>
where
  T: AsyncRead + AsyncWrite + Sized + 'static,
  D: Decoder + 'static,
  D::Error: Into<Error>,
  D::Item: Send + 'static,
  E: Encoder<EI> + 'static + Send,
  EI: 'static + Send,
  E::Error: Into<Error>
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
  pub fn new(name: String, socket: T, decoder: D, encoder: E, client_role: bool) -> Spork<T, D, E, EI> {
    let (read, write) = socket.split();
    let read = FramedRead::new(read.compat(), TaggedDecoder::new(decoder));
    let write = FramedWrite::new(write.compat_write(), TaggedEncoder::new(encoder));
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
  /// # use futures::future::*;
  /// # use futures::executor::*;
  /// # use futures::future;
  /// # use futures::io::{ReadHalf, WriteHalf, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
  /// # use futures::sink::*;
  /// # use futures::stream::*;
  /// # use log::debug;
  /// # use tokio_test::io::Builder;
  /// # use spork::*;
  /// # use spork::message::*;
  /// # use spork::errors::*;
  /// # use std::io;
  /// # use tokio_util::codec::{Decoder, Encoder};
  /// # fn do_spawn<T, D, E, EI>(spork: Spork<T, D, E, EI>)
  /// # where
  /// #   T: AsyncRead + AsyncWrite + Sized + std::marker::Send + 'static,
  /// #   D: Decoder + std::marker::Send + 'static,
  /// #   D::Error: Into<Error> + std::marker::Send,
  /// #   D::Item: std::marker::Send,
  /// #   E: Encoder<EI> + std::marker::Send + 'static,
  /// #   E::Error: Into<Error> + std::marker::Send,
  /// #   EI: std::marker::Send + 'static
  /// # {
  /// let (chat, msgs, driver, _) = spork.process();
  /// block_on(driver).unwrap();
  /// # }
  /// # fn main() {}
  /// ```
  ///
  /// See the package documentation or tests for examples of how to use this function.
  pub fn process(
    self
  ) -> (
    Chatter<D::Item, EI>,
    impl Stream<Item = Result<Response<D::Item, EI>>>,
    impl Future<Output = Result<()>>,
    oneshot::Sender<()>
  ) {
    let Spork { name, read, write, client_role } = self;
    let channels = Channels::new(if client_role { 0 } else { 1 });

    let (in_send, in_recv) = channel(2);
    let (write_in, write_ftr) = out_pump(write);
    let read_ftr = in_pump(read, in_send, channels.clone());
    let chatter = Chatter::new(write_in, channels);
    let (intr_send, intr_recv) = oneshot::channel();

    let chatter_clone = chatter.clone();
    let incoming_resp = in_recv.map(move |m| Ok(Response::new(chatter_clone.clone(), m)));

    (chatter, incoming_resp, combine_rw(name, Box::pin(read_ftr), Box::pin(write_ftr), intr_recv), intr_send)
  }
}

async fn combine_rw<R, W>(name: String, read_ftr: R, write_ftr: W, intr: oneshot::Receiver<()>) -> Result<()>
where
  R: Future<Output = Result<()>> + Unpin,
  W: Future<Output = Result<()>> + Unpin
{
  // We can't allow the read to complete if write completes (or vice versa), since it's likely that it will
  // continue forever, never completing with its error.

  let double = try_select(read_ftr, write_ftr)
    .map_ok(|_| {
      debug!("Pump for \"{}\" done successfully.", name);
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

fn out_pump<T, E, EI>(write: SporkWrite<T, E, EI>) -> (Sender<Closing<Tagged<EI>>>, impl Future<Output = Result<()>>)
where
  T: AsyncRead + AsyncWrite + Sized + 'static,
  E: Encoder<EI> + 'static + Send,
  E::Error: Into<Er>,
  EI: 'static + Send
{
  let (send, rcv) = channel(2);
  (send, out_loop(write, rcv))
}

async fn out_loop<T, E, EI>(mut write: SporkWrite<T, E, EI>, mut rcv: Receiver<Closing<Tagged<EI>>>) -> Result<()>
where
  T: AsyncRead + AsyncWrite + Sized + 'static,
  E: Encoder<EI> + 'static + Send,
  E::Error: Into<Er>,
  EI: 'static + Send
{
  while let Some(val) = rcv.next().await {
    match val {
      Closing::Close => rcv.close(),
      Closing::Item(val) => write.send(val).await.map_err(|e| e.into())?
    }
  }
  Ok(())
}

/// Listen on our reader, and send each received item to the appropriate channel.
async fn in_pump<S, DI, E>(mut read: S, mut new_key: Sender<Tagged<DI>>, mut channels: Channels<DI>) -> Result<()>
where
  DI: 'static + Send,
  S: Stream<Item = std::result::Result<Tagged<DI>, E>> + Unpin,
  E: Into<Error>
{
  while let Some(val) = read.next().await {
    handle_next::<DI>(&mut new_key, &mut channels, val.map_err(|e| e.into())?).await?
  }

  Ok(())
}

async fn handle_next<DI: 'static>(
  new_key: &mut Sender<Tagged<DI>>, channels: &mut Channels<DI>, val: Tagged<DI>
) -> Result<()> {
  let tag = val.tag();
  let channel: Option<Channel<DI>> = channels.remove(tag);

  match channel {
    Some(channel) => channel.send(val).await,
    None => new_key.send(val).await.map_err(|e| e.into())
  }
}

/// A simple message sender/receiver that can initiate conversations from this side of a connection.
pub struct Chatter<DI, EI> {
  write: Sender<Closing<Tagged<EI>>>,
  channels: Channels<DI>
}

impl<DI, EI> Clone for Chatter<DI, EI> {
  fn clone(&self) -> Chatter<DI, EI> { Chatter { write: self.write.clone(), channels: self.channels.clone() } }
}

impl<DI, EI> Chatter<DI, EI>
where
  EI: 'static,
  DI: 'static
{
  fn new(write: Sender<Closing<Tagged<EI>>>, channels: Channels<DI>) -> Chatter<DI, EI> { Chatter { write, channels } }

  /// Close the communication channel.
  pub async fn close(&mut self) -> Result<()> { Ok(self.write.send(Closing::Close).await?) }

  /// Send a message to the server, without expecting a particular response.
  pub fn say<M: Into<EI>>(&mut self, message: M) -> impl Future<Output = Result<()>> + '_ {
    let next_key = self.channels.next_key();
    self.say_tagged(Tagged::new(next_key, message.into()))
  }

  /// Send a message to the server, and return a future that has the response message.
  pub fn ask<M: Into<EI>>(&mut self, message: M) -> impl Future<Output = Result<Response<DI, EI>>> + '_ {
    let next_key = self.channels.next_key();
    self.ask_tagged(Tagged::new(next_key, message.into()))
  }

  /// Send a message to the server, without expecting a particular response.
  pub fn into_say<M: Into<EI>>(self, message: M) -> impl Future<Output = Result<()>> {
    let next_key = self.channels.next_key();
    self.into_say_tagged(Tagged::new(next_key, message.into()))
  }

  /// Send a message to the server, and return a future that has the response message.
  pub fn into_ask<M: Into<EI>>(self, message: M) -> impl Future<Output = Result<Response<DI, EI>>> {
    let next_key = self.channels.next_key();
    self.into_ask_tagged(Tagged::new(next_key, message.into()))
  }

  fn ask_tagged(&mut self, message: Tagged<EI>) -> impl Future<Output = Result<Response<DI, EI>>> + '_ {
    let (sender, receiver) = oneshot::channel();
    let read = timeout(
      Duration::from_secs(ASK_TIMEOUT_SECS),
      receiver.map(|v| v.map_err(|e| bad!("Cancelled while asking: {:?}", e)))
    );
    self.channels.insert(message.tag(), sender);
    let self_clone = self.clone();

    async move {
      self.say_tagged(message).await?;
      let m = read.await??;
      Ok(Response::new(self_clone, m))
    }
  }

  async fn say_tagged(&mut self, message: Tagged<EI>) -> Result<()> {
    Ok(self.write.send(Closing::Item(message)).await?)
  }

  fn into_ask_tagged(self, message: Tagged<EI>) -> impl Future<Output = Result<Response<DI, EI>>> {
    let (sender, receiver) = oneshot::channel();
    let read = timeout(
      Duration::from_secs(ASK_TIMEOUT_SECS),
      receiver.map(|v| v.map_err(|e| bad!("Cancelled while asking: {:?}", e)))
    );
    self.channels.insert(message.tag(), sender);
    let self_clone = self.clone();

    async move {
      self.into_say_tagged(message).await?;
      let m = read.await??;
      Ok(Response::new(self_clone, m))
    }
  }

  async fn into_say_tagged(mut self, message: Tagged<EI>) -> Result<()> {
    Ok(self.write.send(Closing::Item(message)).await?)
  }
}

/// A message from the other side of a connection, in response to a `Chatter::ask` or `Response::ask` query from
/// this side. The response can be used to continue sending messages.
pub struct Response<DI, EI> {
  chatter: Chatter<DI, EI>,
  message: Tagged<DI>
}

impl<DI, EI> Response<DI, EI>
where
  DI: 'static,
  EI: 'static
{
  fn new(chatter: Chatter<DI, EI>, message: Tagged<DI>) -> Response<DI, EI> { Response { chatter, message } }

  pub fn message(&self) -> &DI { &self.message.message() }

  pub fn split(self) -> (Responder<DI, EI>, DI) {
    (Responder::new(self.chatter, self.message.tag()), self.message.into_message())
  }

  /// Close communication.
  pub async fn close(&mut self) -> Result<()> { self.chatter.close().await }

  /// Send a message to the server, without expecting a particular response.
  pub fn say<M: Into<EI>>(self, message: M) -> impl Future<Output = Result<()>> {
    let tag = self.message.tag();
    self.chatter.into_say_tagged(Tagged::new(tag, message.into()))
  }

  /// Send a message to the server, and return a future that has the response message.
  pub fn ask<M: Into<EI>>(self, message: M) -> impl Future<Output = Result<Response<DI, EI>>> {
    let tag = self.message.tag();
    self.chatter.into_ask_tagged(Tagged::new(tag, message.into()))
  }
}

/// The responder component of a response to a `Chatter::ask` or `Responder::ask` query. The responder doesn't
/// contain the answering message, but can be used to continue sending messages.
pub struct Responder<DI, EI>
where
  DI: 'static,
  EI: 'static
{
  chatter: Chatter<DI, EI>,
  tag: u32
}

impl<DI, EI> Clone for Responder<DI, EI>
where
  DI: 'static,
  EI: 'static
{
  fn clone(&self) -> Responder<DI, EI> { Responder { chatter: self.chatter.clone(), tag: self.tag } }
}

impl<DI, EI> Responder<DI, EI>
where
  DI: 'static,
  EI: 'static
{
  fn new(chatter: Chatter<DI, EI>, tag: u32) -> Responder<DI, EI> { Responder { chatter, tag } }

  /// Close communication.
  pub async fn close(&mut self) -> Result<()> { self.chatter.close().await }

  /// Get the chatter that originated this response, so that further conversations can be had.
  pub fn chatter(&self) -> Chatter<DI, EI> { self.chatter.clone() }

  /// Send a message to the server, without expecting a particular response.
  pub fn say<M: Into<EI>>(self, message: M) -> impl Future<Output = Result<()>> {
    let tag = self.tag;
    self.chatter.into_say_tagged(Tagged::new(tag, message.into()))
  }

  /// Send a message to the server, and return a future that has the response message.
  pub fn ask<M: Into<EI>>(self, message: M) -> impl Future<Output = Result<Response<DI, EI>>> {
    let tag = self.tag;
    self.chatter.into_ask_tagged(Tagged::new(tag, message.into()))
  }

  /// Create a new response coming back from the server, based on this responder. See `Response::split`.
  pub fn join(self, message: DI) -> Response<DI, EI> { Response::new(self.chatter, Tagged::new(self.tag, message)) }
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

  fn send(self, message: Tagged<T>) -> Ready<Result<()>> {
    future::ready(self.sender.send(message).map_err(|_| bad!("Couldn't send.")))
  }
}

enum Closing<T> {
  Close,
  Item(T)
}

#[cfg(test)]
mod tests {
  use super::*;
  use futures::future::{try_join, Either};
  use futures::stream::TryStreamExt;
  use message::*;
  use tokio::runtime::Runtime;
  use tokio_test::io::Builder;
  use tokio_util::compat::Tokio02AsyncReadCompatExt;

  #[test]
  fn test_chatter_say() {
    let mut chatter = setup_chatter();
    let channels = chatter.channels.channels.clone();
    drop(chatter.say("Sending without expecting response."));
    assert_eq!(0, channels.lock().unwrap().len());
  }

  fn block_on<F: Future>(f: F) -> F::Output { Runtime::new().unwrap().block_on(f) }

  #[test]
  fn test_chatter_ask() {
    let mut chatter = setup_chatter();
    let channels = chatter.channels.channels.clone();
    drop(block_on(async move { chatter.ask("What is the response?").await }));
    assert_eq!(1, channels.lock().unwrap().len());
  }

  #[test]
  fn test_outgoing() {
    block_on(async move {
      let mocket = Builder::new()
        .write(b"\x00\x00\x00\x00\x00\x00\x00\x09Sending 1")
        .read(b"\x00\x00\x00\x00\x00\x00\x00\x05Got 1")
        .build()
        .compat();
      let verification = Arc::new(Mutex::new(Vec::new()));

      let client = Spork::new("test".into(), mocket, Dec::new(), Enc::new(), true);
      let (mut client_chat, _, driver, _) = client.process();

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
            _ => future::err(bad!("Bad from client."))
          }
        })
        .map_ok(|_| verification.lock().unwrap().push(3));

      try_join(client_comm, driver).map(|r| r.map(|_| ())).await.unwrap();

      let verification = verification.lock().unwrap();
      assert_eq!(verification.as_slice(), &[1, 2, 3]);
      assert_eq!(0, channels.lock().unwrap().len());
    });
  }

  #[test]
  fn test_outgoing_async_keyword() {
    let mocket = Builder::new()
      .write(b"\x00\x00\x00\x00\x00\x00\x00\x09Sending 1")
      .read(b"\x00\x00\x00\x00\x00\x00\x00\x05Got 1")
      .build()
      .compat();
    let verification = Arc::new(Mutex::new(Vec::new()));

    let client = Spork::new("test".into(), mocket, Dec::new(), Enc::new(), true);
    let (mut client_chat, _, driver, _) = client.process();

    let channels = client_chat.channels.channels.clone();
    let client_comm = async {
      let response = client_chat.ask("Sending 1").await?;
      verification.lock().unwrap().push(1);
      if response.message().text() != "Got 1" {
        return err!("Bad from client.");
      } else {
        verification.lock().unwrap().push(2);
      }
      Ok(verification.lock().unwrap().push(3))
    };

    block_on(try_join(client_comm, driver).map(|r| r.map(|_| ()))).unwrap();
    let verification = verification.lock().unwrap();
    assert_eq!(verification.as_slice(), &[1, 2, 3]);
    assert_eq!(0, channels.lock().unwrap().len());
  }

  #[test]
  fn test_incoming() {
    let mocket = Builder::new()
      .read(b"\x00\x00\x00\x00\x00\x00\x00\x09Sending 1")
      .write(b"\x00\x00\x00\x00\x00\x00\x00\x05Got 1")
      .build()
      .compat();
    let verification = Arc::new(Mutex::new(Vec::new()));

    let server = Spork::new("test".into(), mocket, Dec::new(), Enc::new(), false);
    let (server_chat, server_comm, driver, _) = server.process();

    let server_comm = server_comm
      .try_for_each(|msg| {
        verification.lock().unwrap().push(1);
        match msg.message().text() {
          "Sending 1" => {
            verification.lock().unwrap().push(2);
            Either::Left(msg.say("Got 1"))
          }
          _ => Either::Right(future::err(bad!("Bad msg from server.")))
        }
      })
      .map_ok(|_| verification.lock().unwrap().push(3));

    block_on(try_join(server_comm, driver).map(|r| r.map(|_| ()))).unwrap();
    let verification = verification.lock().unwrap();
    assert_eq!(verification.as_slice(), &[1, 2, 3]);
    assert_eq!(0, server_chat.channels.channels.lock().unwrap().len());
  }

  fn setup_chatter() -> Chatter<Dec, Message> {
    let (write, _) = channel(0);
    let channels = Channels::new(0);
    Chatter::new(write, channels)
  }
}
