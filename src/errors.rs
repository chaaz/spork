//! Error handling for Versio is all based on `error-chain`.

use error_chain::error_chain;

error_chain! {
  links {
  }

  foreign_links {
    Addr(std::net::AddrParseError);
    Io(std::io::Error);
    Num(std::num::ParseIntError);
    Utf(std::str::Utf8Error);
    MpscSend(futures::channel::mpsc::SendError);
  }
}

impl<'a, T: ?Sized> From<std::sync::PoisonError<std::sync::MutexGuard<'a, T>>> for Error {
  fn from(err: std::sync::PoisonError<std::sync::MutexGuard<'a, T>>) -> Error {
    format!("serde yaml error {:?}", err).into()
  }
}

#[macro_export]
macro_rules! err {
  ($($arg:tt)*) => (
    std::result::Result::Err($crate::errors::Error::from_kind($crate::errors::ErrorKind::Msg(format!($($arg)*))))
  )
}

#[macro_export]
macro_rules! bad {
  ($($arg:tt)*) => ($crate::errors::Error::from_kind($crate::errors::ErrorKind::Msg(format!($($arg)*))))
}
