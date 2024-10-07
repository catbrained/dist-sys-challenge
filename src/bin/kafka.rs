use std::collections::HashMap;

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
        unacknowledged: Vec::new(),
        logs: HashMap::new(),
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
                    m.send(&mut output).await.context("Failed to send retry")?
                }
            }
        };
    }
}

#[derive(Debug, Default)]
struct Log {
    next_offset: u64,
    committed: u64,
    messages: Vec<(u64, u64)>,
}

struct Node {
    id: Option<String>,
    msg_id: u64,
    unacknowledged: Vec<Message>,
    logs: HashMap<String, Log>,
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
            InnerMessageBody::Send { key, msg: m } => {
                let l = self.logs.entry(key).or_default();
                l.messages.push((l.next_offset, m));
                let offset = l.next_offset;
                l.next_offset += 1;
                let reply = Message {
                    src: self.id.as_ref().unwrap().clone(),
                    dst: msg.src,
                    body: MessageBody {
                        id: Some(self.msg_id),
                        in_reply_to: msg.body.id,
                        inner: InnerMessageBody::SendOk { offset },
                    },
                };
                reply.send(output).await?;
                self.msg_id += 1;
            }
            InnerMessageBody::Poll { offsets } => {
                let mut msgs = HashMap::new();
                for (k, o) in offsets {
                    // Clients sometimes poll for logs that we don't know about.
                    if let Some(log) = self.logs.get(&k) {
                        let messages: Vec<(u64, u64)> = log
                            .messages
                            .iter()
                            .skip_while(|(offset, _)| *offset < o)
                            // We're limiting the number of messages to return per poll. The value is arbitrary.
                            .take(20)
                            .cloned()
                            .collect();
                        msgs.insert(k, messages);
                    }
                }
                let reply = Message {
                    src: self.id.as_ref().unwrap().clone(),
                    dst: msg.src,
                    body: MessageBody {
                        id: Some(self.msg_id),
                        in_reply_to: msg.body.id,
                        inner: InnerMessageBody::PollOk { msgs },
                    },
                };
                reply.send(output).await?;
                self.msg_id += 1;
            }
            InnerMessageBody::CommitOffsets { offsets } => {
                for (k, o) in offsets {
                    self.logs.entry(k).and_modify(|l| l.committed = o);
                }
                let reply = Message {
                    src: self.id.as_ref().unwrap().clone(),
                    dst: msg.src,
                    body: MessageBody {
                        id: Some(self.msg_id),
                        in_reply_to: msg.body.id,
                        inner: InnerMessageBody::CommitOffsetsOk,
                    },
                };
                reply.send(output).await?;
                self.msg_id += 1;
            }
            InnerMessageBody::ListCommittedOffsets { keys } => {
                let mut offsets = HashMap::new();
                for k in keys {
                    if let Some(log) = self.logs.get(&k) {
                        offsets.insert(k, log.committed);
                    }
                }
                let reply = Message {
                    src: self.id.as_ref().unwrap().clone(),
                    dst: msg.src,
                    body: MessageBody {
                        id: Some(self.msg_id),
                        in_reply_to: msg.body.id,
                        inner: InnerMessageBody::ListCommittedOffsetsOk { offsets },
                    },
                };
                reply.send(output).await?;
                self.msg_id += 1;
            }
            InnerMessageBody::Error { code, text } => {
                panic!("Encountered error message with code {code} and message {text:?}");
            }
            _ => {
                // NOTE: Let's assume that everyone is behaving nicely and we don't get
                // any `InitOk`s or other messages that we don't expect. :)
                unreachable!("unexpected message type encountered")
            }
        }

        Ok(())
    }
}
