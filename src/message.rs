//! A simple message format, along with an associated encoder and decoder.

use bytes::{Buf, BufMut, BytesMut, IntoBuf};
use std::io;
use tokio::codec::{Decoder, Encoder};

/// A simple message that just wraps a `String`.
pub struct Message {
  message: String
}

impl Message {
  pub fn new(message: &str) -> Message { Message { message: message.into() } }
  pub fn text(&self) -> &str { &self.message }
}

impl From<&str> for Message {
  fn from(v: &str) -> Message { Message::new(v) }
}

/// A simple encoder that writes a big-endian u32 byte length, followed by the utf8 bytes of the string.
pub struct Enc {}
impl Enc {
  pub fn new() -> Enc { Enc {} }
}

impl Encoder for Enc {
  type Item = Message;
  type Error = io::Error;
  fn encode(&mut self, message: Message, buf: &mut BytesMut) -> io::Result<()> {
    let bytes = message.text().as_bytes();
    buf.put_u32_be(bytes.len() as u32);
    buf.extend(bytes);
    Ok(())
  }
}

/// A simple decoder that reads a big-endian u32 byte length, followed by the utf8 bytes of the string.
pub struct Dec {
  len: Option<u32>
}

impl Dec {
  pub fn new() -> Dec { Dec { len: None } }
}

impl Decoder for Dec {
  type Item = Message;
  type Error = io::Error;
  fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Message>> {
    if let Some(len) = self.len {
      if (buf.len() as u32) < len {
        Ok(None)
      } else {
        self.len = None;
        Ok(Some(Message::new(::std::str::from_utf8(&buf.split_to(len as usize).to_vec()).unwrap())))
      }
    } else {
      if buf.len() < 4 {
        Ok(None)
      } else {
        self.len = Some(buf.split_to(4).into_buf().get_u32_be());
        self.decode(buf)
      }
    }
  }
}
