//! The encoder/decoder code for a spork object.

use bytes::{Buf, BufMut, BytesMut, IntoBuf};
use tokio_codec::{Decoder, Encoder};

pub struct Keyed<T> {
  key: u32,
  t: T
}

impl<T> Keyed<T> {
  pub fn new(key: u32, t: T) -> Keyed<T> { Keyed { key, t } }
  pub fn key(&self) -> u32 { self.key }
  pub fn into_t(self) -> T { self.t }
}

pub struct KeyedEncoder<E> {
  e: E
}

impl<E> KeyedEncoder<E> {
  pub fn new(e: E) -> KeyedEncoder<E> { KeyedEncoder { e } }
}

impl<E> Encoder for KeyedEncoder<E> 
where
  E: Encoder
{
  type Item = Keyed<E::Item>;
  type Error = E::Error;

  fn encode(&mut self, i: Keyed<E::Item>, buf: &mut BytesMut) -> Result<(), E::Error> {
    buf.put_u32_be(i.key());
    self.e.encode(i.into_t(), buf)
  }
}

pub struct KeyedDecoder<D> {
  key: Option<u32>,
  d: D
}

impl<D> KeyedDecoder<D> {
  pub fn new(d: D) -> KeyedDecoder<D> { KeyedDecoder { key: None, d } }
}

impl<D> Decoder for KeyedDecoder<D> 
where
  D: Decoder
{
  type Item = Keyed<D::Item>;
  type Error = D::Error;

  fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Keyed<D::Item>>, D::Error> {
    if let Some(key) = self.key {
      match self.d.decode(buf) {
        Ok(Some(v)) => {
          self.key = None;
          Ok(Some(Keyed::new(key, v)))
        }
        Ok(None) => Ok(None),
        Err(e) => Err(e)
      }
    } else {
      if buf.len() < 4 {
        Ok(None)
      } else {
        self.key = Some(get_u32(buf));
        self.decode(buf)
      }
    }
  }
}

/// Gets a u32 from a buffer that has one.
pub fn get_u32(buf: &mut BytesMut) -> u32 { buf.split_to(4).into_buf().get_u32_be() }
