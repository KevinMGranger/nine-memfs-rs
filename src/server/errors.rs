use failure::*;
use nine::common::CowStr;
use std::fmt::{self, Display};

/// TODO: this can be its own enum.
/// All possible protocol-level errors could be enumerated,
/// and have a single one for a custom message.
/// or maybe that could be its own serverfail variant.
#[derive(Debug)]
pub struct NonFatalError {
    pub msg: CowStr,
    cause: Option<Error>,
}

impl Display for NonFatalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl Fail for NonFatalError {
    fn cause(&self) -> Option<&Fail> {
        self.cause.as_ref().map(|x| x.as_fail())
    }
    fn backtrace(&self) -> Option<&Backtrace> {
        self.cause.as_ref().map(|x| x.backtrace())
    }
}

#[derive(Debug)]
pub enum ServerFail {
    /// An error message should be returned to the client,
    /// but the server should continue as usual.
    /// Typically used in response to client errors.
    /// Could also be used if there was an error server-side
    /// but the connection should not be affected
    /// (e.g. an io error on a single file).
    NonFatal(NonFatalError),
    /// The error message will be converted to an Rerror,
    /// sent, and then the server will shut down.
    NotifiedFatal(Error),
    /// The server will immediately shut down.
    /// Nothing is sent to the client.
    ImmediateFatal(Error),
}

impl Display for ServerFail {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::ServerFail::*;
        match self {
            NonFatal(x) => write!(f, "NonFatalError: {}", x),
            NotifiedFatal(x) => write!(f, "Notified Fatal Error: {}", x),
            ImmediateFatal(x) => write!(f, "Immediate Fatal Error: {}", x),
        }
    }
}

impl Fail for ServerFail {
    fn cause(&self) -> Option<&Fail> {
        use self::ServerFail::*;
        match self {
            NonFatal(x) => x.cause(),
            NotifiedFatal(x) => Some(x.as_fail()),
            ImmediateFatal(x) => Some(x.as_fail()),
        }
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        use self::ServerFail::*;
        match self {
            NonFatal(x) => x.backtrace(),
            NotifiedFatal(x) => Some(x.backtrace()),
            ImmediateFatal(x) => Some(x.backtrace()),
        }
    }
}

pub type NineResult<T> = Result<T, ServerFail>;

pub fn rerr<T, S: Into<CowStr>>(s: S) -> NineResult<T> {
    Err(ServerFail::NonFatal(NonFatalError {
        msg: s.into(),
        cause: None,
    }))
}
