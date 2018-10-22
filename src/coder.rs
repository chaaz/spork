//! The encoder/decoder instrumentation for a spork object.

use bytes::{Buf, BufMut, BytesMut, IntoBuf};
use tokio_codec::{Decoder, Encoder};

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
  pub fn message_mut(&mut self) -> &mut M { &mut self.message }
}

pub struct TaggedEncoder<E> {
  e: E
}

impl<E> TaggedEncoder<E> {
  pub fn new(e: E) -> TaggedEncoder<E> { TaggedEncoder { e } }
}

impl<E> Encoder for TaggedEncoder<E>
where
  E: Encoder
{
  type Item = Tagged<E::Item>;
  type Error = E::Error;

  fn encode(&mut self, i: Tagged<E::Item>, buf: &mut BytesMut) -> Result<(), E::Error> {
    buf.put_u32_be(i.tag());
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
    } else {
      if buf.len() < 4 {
        Ok(None)
      } else {
        self.tag = Some(get_u32(buf));
        self.decode(buf)
      }
    }
  }
}

/// Get a u32 from a buffer that has one.
pub fn get_u32(buf: &mut BytesMut) -> u32 { buf.split_to(4).into_buf().get_u32_be() }

/// Unit tests.
#[cfg(test)]
mod tests {
  use super::*;

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
  fn tagged_message_mut() {
    let mut tagged = Tagged::new(3, "Bob".to_string());
    tagged.message_mut().push_str(" is great.");
    assert_eq!(tagged.message().as_str(), "Bob is great.");
  }
}
