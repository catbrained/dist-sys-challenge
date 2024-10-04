use anyhow::Result;
use tokio::io;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

use dist_sys_challenge::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let codec = LinesCodec::new();
    let mut input = FramedRead::new(stdin, codec.clone());
    let mut output = FramedWrite::new(stdout, codec);

    let mut node = Node {
        id: None,
        msg_id: 1,
    };

    while let Some(line) = input.try_next().await? {
        let message: Message = serde_json::from_str(&line)?;
        node.handle_msg(message, &mut output).await?;
    }

    Ok(())
}

struct Node {
    id: Option<String>,
    msg_id: u64,
}

impl Node {
    async fn handle_msg(
        &mut self,
        msg: Message,
        output: &mut FramedWrite<io::Stdout, LinesCodec>,
    ) -> Result<()> {
        // NOTE: I'm assuming that all messages we receive are actually intended for us
        // and thus we don't need to check the destination value matches our id.
        match msg.body.inner {
            InnerMessageBody::Init { node_id, .. } => {
                self.id = self
                    .id
                    .as_ref()
                    // TODO: should we respond with an error message instead of panicking?
                    .map(|_| panic!("Node id is already set, but we received an init message"))
                    .or(Some(node_id));
                // TODO: the node IDs sent in the init message are ignored for now,
                // since we're not using them for anything at the moment.
                let reply = Message {
                    src: msg.dst,
                    dst: msg.src,
                    body: MessageBody {
                        id: Some(self.msg_id),
                        in_reply_to: msg.body.id,
                        inner: InnerMessageBody::InitOk,
                    },
                };
                reply.send(output).await?;
                self.msg_id += 1;
            }
            InnerMessageBody::Generate => {
                let reply = Message {
                    src: msg.dst,
                    dst: msg.src,
                    body: MessageBody {
                        id: Some(self.msg_id),
                        in_reply_to: msg.body.id,
                        inner: InnerMessageBody::GenerateOk {
                            // Since both our node ID and our current message ID are unique,
                            // we can combine them to create a globally unique ID.
                            // Note that this assumption breaks in conditions where node IDs can be
                            // reused. For example, in a scenario where nodes can crash/stop and new nodes
                            // can later join the cluster and receive a previously used ID.
                            // This scheme also has other properties that may be undesireable for certain
                            // applications: the IDs are sequential (per node), and the IDs allow identifying the
                            // origin node that generated it.
                            // Depending on the requirements, it may be better to use a scheme such as the ones
                            // described in RFC9562 (see: https://datatracker.ietf.org/doc/html/rfc9562).
                            id: format!("{}-{}", self.id.as_ref().unwrap(), self.msg_id),
                        },
                    },
                };
                reply.send(output).await?;
                self.msg_id += 1;
            }
            _ => {
                // NOTE: Let's assume that everyone is behaving nicely and we don't get
                // any `InitOk`s or other messages that we don't expect. :)
                unreachable!()
            }
        }

        Ok(())
    }
}
