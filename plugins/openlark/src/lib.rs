//! `steve-plugin-openlark`: a Steve channel plugin that lets the Hub use Feishu / Lark as the
//! signed-in user.
//!
//! Towards the Hub it is a server of Steve's external channel protocol v1 (`ext`); towards Lark
//! it is a client of an openlark-gateway (`gateway`). One process bridges one gateway
//! credential, which is one signed-in user. References like §5.4 in the comments are sections
//! of the external channel protocol.

pub mod bridge;
pub mod config;
pub mod ext;
pub mod feed;
pub mod gateway;
pub mod history;
pub mod map;
pub mod media;
pub mod reads;
pub mod store;
pub mod writes;

pub use bridge::run;
