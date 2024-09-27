use serde::{Deserialize, Serialize};

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
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
}
