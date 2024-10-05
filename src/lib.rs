use std::collections::HashMap;

use anyhow::Result;
use futures::SinkExt;
use serde::{Deserialize, Serialize};
use tokio::io;
use tokio_util::codec::{FramedWrite, LinesCodec};

// TODO: maybe we should implement some convenience functions,
// like, e.g., `reply` to handle swapping src and dst etc.
#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: MessageBody,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MessageBody {
    #[serde(rename = "msg_id")]
    pub id: Option<u64>,
    pub in_reply_to: Option<u64>,
    #[serde(flatten)]
    pub inner: InnerMessageBody,
}

/// The part of the message that is specific to each
/// type of message.
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum InnerMessageBody {
    // Common message types
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Error {
        // TODO: implement the Maelstrom error codes?
        // See: https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#errors
        code: u16,
        text: Option<String>,
    },
    // 1. Echo challenge
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    // 2. Unique ID generation challenge
    Generate,
    GenerateOk {
        id: String,
    },
    // 3. Broadcast challenge
    Broadcast {
        message: u64,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<u64>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    // 3e. Custom message for efficient broadcast
    BatchBroadcast {
        messages: Vec<u64>,
    },
}

impl Message {
    /// Serialize and send the message in a newline delimited way, as the Maelstrom protocol expects.
    pub async fn send(&self, output: &mut FramedWrite<io::Stdout, LinesCodec>) -> Result<()> {
        let msg = serde_json::to_string(self)?;
        output.send(msg).await?;
        Ok(())
    }
}
