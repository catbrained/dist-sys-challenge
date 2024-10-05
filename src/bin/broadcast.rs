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
        nodes: Vec::new(),
        unacknowledged: Vec::new(),
    };

    let start = Instant::now() + Duration::from_millis(1000);
    // NOTE: This interval seems to produce decent results, but it might be worth
    // experimenting with different values.
    let mut interval = time::interval_at(start, Duration::from_millis(1000));
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
    nodes: Vec<String>,
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
            InnerMessageBody::Init {
                node_id,
                mut node_ids,
            } => {
                self.id = self
                    .id
                    .as_ref()
                    // TODO: should we respond with an error message instead of panicking?
                    .map(|_| panic!("Node id is already set, but we received an init message"))
                    .or(Some(node_id));
                node_ids.sort();
                self.nodes = node_ids;
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
            InnerMessageBody::Topology { .. } => {
                // We ignore the topology suggestion from Maelstrom and build our own.
                // We build a tree with a maximum of `fanout` children per node.
                let fanout = 4;
                let i = self.nodes.binary_search(self.id.as_ref().unwrap()).unwrap();
                let mut children: &[String] = &[];
                let child_idx = fanout * i + 1;
                // Check that we are not a leaf node.
                if child_idx < self.nodes.len() {
                    children = &self.nodes[child_idx..(child_idx + fanout).min(self.nodes.len())];
                }
                let mut parent: &[String] = &[];
                if i != 0 {
                    // We are not the root node, so get our parent.
                    let parent_idx = (i - 1) / fanout;
                    parent = &self.nodes[parent_idx..(parent_idx + 1)];
                }
                self.neighbors = [parent, children].concat();
                debug_assert!(self.neighbors.len() <= fanout + 1);
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
