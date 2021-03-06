//! The encoder/decoder instrumentation for a spork object.

use bytes::{Buf, BufMut, BytesMut};
use std::marker::PhantomData;
use tokio_util::codec::{Decoder, Encoder};

/// A tagged message, which wraps an original message.
pub struct Tagged<M> {
  tag: u32,
  message: M
}

impl<M> Tagged<M> {
  pub fn new(tag: u32, message: M) -> Tagged<M> { Tagged { tag, message } }

  pub fn tag(&self) -> u32 { self.tag }
  pub fn into_message(self) -> M { self.message }
  pub fn message(&self) -> &M { &self.message }
}

pub struct TaggedEncoder<E, EI>
where
  E: Encoder<EI> + 'static + Send,
  EI: 'static + Send
{
  e: E,
  _ei: PhantomData<fn() -> EI>
}

impl<E, EI> TaggedEncoder<E, EI>
where
  E: Encoder<EI> + 'static + Send,
  EI: 'static + Send
{
  pub fn new(e: E) -> TaggedEncoder<E, EI> { TaggedEncoder { e, _ei: PhantomData } }
}

impl<E, EI> Encoder<Tagged<EI>> for TaggedEncoder<E, EI>
where
  E: Encoder<EI> + 'static + Send,
  EI: 'static + Send
{
  type Error = E::Error;

  fn encode(&mut self, i: Tagged<EI>, buf: &mut BytesMut) -> Result<(), E::Error> {
    if buf.remaining_mut() < 4 {
      buf.reserve(4);
    }
    buf.put_u32(i.tag());
    self.e.encode(i.into_message(), buf)
  }
}

pub struct TaggedDecoder<D> {
  tag: Option<u32>,
  d: D
}

impl<D> TaggedDecoder<D> {
  pub fn new(d: D) -> TaggedDecoder<D> { TaggedDecoder { tag: None, d } }
}

impl<D> Decoder for TaggedDecoder<D>
where
  D: Decoder
{
  type Item = Tagged<D::Item>;
  type Error = D::Error;

  fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Tagged<D::Item>>, D::Error> {
    if let Some(tag) = self.tag {
      match self.d.decode(buf) {
        Ok(Some(v)) => {
          self.tag = None;
          Ok(Some(Tagged::new(tag, v)))
        }
        Ok(None) => Ok(None),
        Err(e) => Err(e)
      }
    } else if buf.len() < 4 {
      Ok(None)
    } else {
      self.tag = Some(get_u32(buf));
      self.decode(buf)
    }
  }
}

/// Get a u32 from a buffer that has one.
pub fn get_u32(buf: &mut BytesMut) -> u32 { buf.split_to(4).get_u32() }

#[cfg(test)]
mod tests {
  use super::*;
  use crate::message::*;
  use std::ops::Deref;

  #[test]
  fn tagged_tag() {
    let tagged = Tagged::new(3, "Bob");
    assert_eq!(tagged.tag(), 3);
  }

  #[test]
  fn tagged_into_message() {
    let tagged = Tagged::new(3, "Bob");
    assert_eq!(tagged.into_message(), "Bob");
  }

  #[test]
  fn tagged_message() {
    let tagged = Tagged::new(3, "Bob".to_string());
    assert_eq!(tagged.message().as_str(), "Bob");
  }

  #[test]
  fn tagged_encoder_enc() {
    let mut buf = BytesMut::new();
    TaggedEncoder::new(Enc::new()).encode(Tagged::new(9, Message::new("stuff")), &mut buf).unwrap();
    assert_eq!(buf.deref(), b"\x00\x00\x00\x09\x00\x00\x00\x05stuff");
  }

  #[test]
  fn tagged_encoder_dec() {
    let mut bytes = BytesMut::new();
    bytes.extend_from_slice(b"\x00\x00\x00\x09\x00\x00\x00\x04what");
    let tagged_msg = TaggedDecoder::new(Dec::new()).decode(&mut bytes).unwrap().unwrap();
    assert_eq!(tagged_msg.tag(), 9);
    assert_eq!(tagged_msg.into_message().text(), "what");
  }
}
