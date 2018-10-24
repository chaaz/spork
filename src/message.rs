//! A simple message format, along with an associated encoder and decoder.

use bytes::{Buf, BufMut, BytesMut, IntoBuf};
use std::io;
use tokio_codec::{Decoder, Encoder};

pub struct Msg {
  msg: String
}

impl Msg {
  pub fn new(msg: &str) -> Msg { Msg { msg: msg.into() } }
  pub fn text(&self) -> &str { &self.msg }
}

pub struct Enc {}
impl Enc {
  pub fn new() -> Enc { Enc {} }
}

impl Encoder for Enc {
  type Item = Msg;
  type Error = io::Error;
  fn encode(&mut self, msg: Msg, buf: &mut BytesMut) -> io::Result<()> {
    buf.put_u32_be(msg.text().len() as u32);
    buf.extend(msg.text().as_bytes());
    Ok(())
  }
}

pub struct Dec {
  len: Option<u32>
}

impl Dec {
  pub fn new() -> Dec { Dec { len: None } }
}

impl Decoder for Dec {
  type Item = Msg;
  type Error = io::Error;
  fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Msg>> {
    if let Some(len) = self.len {
      if (buf.len() as u32) < len {
        Ok(None)
      } else {
        self.len = None;
        Ok(Some(Msg::new(::std::str::from_utf8(&buf.split_to(len as usize).to_vec()).unwrap())))
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
