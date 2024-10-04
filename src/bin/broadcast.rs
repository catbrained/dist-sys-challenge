use std::collections::HashSet;

use anyhow::{anyhow, Context, Result};
use tokio::io;
use tokio::time::{self, Duration, Instant, MissedTickBehavior};
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
        known: HashSet::new(),
        neighbors: Vec::new(),
        unacknowledged: Vec::new(),
    };

    let start = Instant::now() + Duration::from_millis(500);
    // NOTE: This interval seems to produce decent results, but it might be worth
    // experimenting with different values.
    let mut interval = time::interval_at(start, Duration::from_millis(500));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            Some(res) = input.next() => {
                let Ok(ref line) = res else {
                    return Err(anyhow!("Failed to read line from input"));
                };
                let message = serde_json::from_str(line)?;
                node.handle_msg(message, &mut output).await?
            }
            _ = interval.tick() => {
                for m in node.unacknowledged.iter() {
                    m.send(&mut output).await.context("Failed to send gossip retry")?
                }
            }
        };
    }
}

struct Node {
    id: Option<String>,
    msg_id: u64,
    known: HashSet<u64>,
    neighbors: Vec<String>,
    unacknowledged: Vec<Message>,
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
            InnerMessageBody::Broadcast { message } => {
                let already_seen = !self.known.insert(message);
                // Gossip to our neighbors, but only if we haven't seen this value before,
                // to avoid infinite loops.
                if !already_seen {
                    for n in self.neighbors.iter() {
                        // Don't send the message back to the node we received it from.
                        if *n == msg.src {
                            continue;
                        }
                        let gossip = Message {
                            src: self.id.as_ref().unwrap().clone(),
                            dst: n.clone(),
                            body: MessageBody {
                                id: Some(self.msg_id),
                                in_reply_to: None,
                                inner: InnerMessageBody::Broadcast { message },
                            },
                        };
                        self.unacknowledged.push(gossip);
                        self.unacknowledged.last().unwrap().send(output).await?;
                        self.msg_id += 1;
                    }
                }
                let reply = Message {
                    src: msg.dst,
                    dst: msg.src,
                    body: MessageBody {
                        id: Some(self.msg_id),
                        in_reply_to: msg.body.id,
                        inner: InnerMessageBody::BroadcastOk,
                    },
                };
                reply.send(output).await?;
                self.msg_id += 1;
            }
            InnerMessageBody::Topology { topology } => {
                self.neighbors = topology.get(self.id.as_ref().unwrap()).unwrap().clone();
                let reply = Message {
                    src: msg.dst,
                    dst: msg.src,
                    body: MessageBody {
                        id: Some(self.msg_id),
                        in_reply_to: msg.body.id,
                        inner: InnerMessageBody::TopologyOk,
                    },
                };
                reply.send(output).await?;
                self.msg_id += 1;
            }
            InnerMessageBody::Read => {
                let reply = Message {
                    src: msg.dst,
                    dst: msg.src,
                    body: MessageBody {
                        id: Some(self.msg_id),
                        in_reply_to: msg.body.id,
                        inner: InnerMessageBody::ReadOk {
                            messages: self.known.clone().into_iter().collect(),
                        },
                    },
                };
                reply.send(output).await?;
                self.msg_id += 1;
            }
            InnerMessageBody::BroadcastOk => {
                for (i, m) in self.unacknowledged.iter().enumerate() {
                    if m.body.id.unwrap() == msg.body.in_reply_to.unwrap() {
                        self.unacknowledged.remove(i);
                        break;
                    }
                }
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
