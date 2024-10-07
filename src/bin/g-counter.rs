use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use tokio::io;
use tokio::time::{self, Duration, Instant, MissedTickBehavior};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

use dist_sys_challenge::*;

const COUNTER: &str = "counter";
const SEQ_KV: &str = "seq-kv";

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
        request_map: HashMap::new(),
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

struct Node {
    id: Option<String>,
    msg_id: u64,
    unacknowledged: Vec<Message>,
    /// A map from message ID to (client ID, message ID, delta).
    request_map: HashMap<u64, (String, u64, Option<u64>)>,
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
                // Let's initialize the counter in the KV store.
                let init_kv = Message {
                    src: self.id.as_ref().unwrap().to_string(),
                    dst: SEQ_KV.to_owned(),
                    body: MessageBody {
                        id: Some(self.msg_id),
                        in_reply_to: None,
                        inner: InnerMessageBody::CasKv {
                            key: COUNTER.to_owned(),
                            from: "0".to_owned(),
                            to: "0".to_owned(),
                            create_if_not_exists: true,
                        },
                    },
                };
                self.unacknowledged.push(init_kv);
                self.unacknowledged.last().unwrap().send(output).await?;
                self.msg_id += 1;
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
            InnerMessageBody::Read => {
                // Contact the KV store to get the value of the counter
                let kv_request = Message {
                    src: self.id.as_ref().unwrap().to_string(),
                    dst: SEQ_KV.to_owned(),
                    body: MessageBody {
                        id: Some(self.msg_id),
                        in_reply_to: None,
                        // TODO: Do we store one single counter in the KV, or do we
                        // store one counter per node and later combine them?
                        inner: InnerMessageBody::ReadKv {
                            key: COUNTER.to_owned(),
                        },
                    },
                };
                // Once the KV store answers we want to route the answer back to
                // the client who asked us for the value, so we store a mapping
                // from the ID of this message to the name of the client and
                // their message ID we ought to respond to
                // (and set no delta because this is not an Add request).
                self.request_map
                    .insert(self.msg_id, (msg.src, msg.body.id.unwrap(), None));
                self.unacknowledged.push(kv_request);
                self.unacknowledged.last().unwrap().send(output).await?;
                self.msg_id += 1;
            }
            InnerMessageBody::ReadOk(ReadOkVariants::Kv { value }) => {
                let mut duplicate = true;
                // We received a response to our Read request, so mark it as acknowledged.
                for (i, m) in self.unacknowledged.iter().enumerate() {
                    if m.body.id.unwrap() == msg.body.in_reply_to.unwrap() {
                        self.unacknowledged.remove(i);
                        duplicate = false;
                        break;
                    }
                }
                if !duplicate {
                    // Look up if a client is waiting for our answer to their Read request,
                    // and get their ID and the message ID we're replying to.
                    let (client_id, in_reply_to, delta) = self
                        .request_map
                        .remove(&msg.body.in_reply_to.unwrap())
                        .context("in ReadKvOk match arm")
                        .expect("could not retrieve request mapping");
                    // TODO: figure out how we want to store the counter on the KV,
                    // and convert it appropriately.
                    let parsed_value = value
                        .parse()
                        .context("in ReadKvOk match arm")
                        .expect("counter value should have been valid");
                    if let Some(delta) = delta {
                        // Looks like this KV Read request was not issued in response to a client Read request.
                        // We're trying to serve an Add request from a client, and step 1 was to retrieve the
                        // current counter value. Now we can issue a CAS to the KV store to increment the counter.
                        let new_value: u64 = parsed_value + delta;
                        let kv_request = Message {
                            src: self.id.as_ref().unwrap().to_string(),
                            dst: SEQ_KV.to_owned(),
                            body: MessageBody {
                                id: Some(self.msg_id),
                                in_reply_to: None,
                                // TODO: Do we store one single counter in the KV, or do we
                                // store one counter per node and later combine them?
                                inner: InnerMessageBody::CasKv {
                                    key: COUNTER.to_owned(),
                                    from: value,
                                    to: new_value.to_string(),
                                    // Since we already read the value, the key should exist.
                                    // There are no delete operations on the KV store.
                                    // Additionally it is sequentially consistent, which implies monotonic reads.
                                    create_if_not_exists: false,
                                },
                            },
                        };
                        // Once the KV store answers our CAS we want to reply to
                        // the client who asked us to add to the value, so we store a mapping
                        // from the ID of this message to the name of the client,
                        // their message ID we ought to respond to,
                        // and the delta to apply (in case the CAS fails and we need to repeat this dance).
                        self.request_map
                            .insert(self.msg_id, (client_id, in_reply_to, Some(delta)));
                        self.unacknowledged.push(kv_request);
                        self.unacknowledged.last().unwrap().send(output).await?;
                        self.msg_id += 1;
                    } else {
                        let reply = Message {
                            src: self.id.as_ref().unwrap().to_string(),
                            dst: client_id,
                            body: MessageBody {
                                id: Some(self.msg_id),
                                in_reply_to: Some(in_reply_to),
                                inner: InnerMessageBody::ReadOk(ReadOkVariants::Single {
                                    value: parsed_value,
                                }),
                            },
                        };
                        reply.send(output).await?;
                        self.msg_id += 1;
                    }
                }
            }
            InnerMessageBody::Add { delta } => {
                // To add to the counter we first need to get the current value
                // and then issue a CAS.
                // Contact the KV store to get the value of the counter
                let kv_request = Message {
                    src: self.id.as_ref().unwrap().to_string(),
                    dst: SEQ_KV.to_owned(),
                    body: MessageBody {
                        id: Some(self.msg_id),
                        in_reply_to: None,
                        // TODO: Do we store one single counter in the KV, or do we
                        // store one counter per node and later combine them?
                        inner: InnerMessageBody::ReadKv {
                            key: COUNTER.to_owned(),
                        },
                    },
                };
                // Once the KV store answers we want to issue a CAS and then
                // once the KV store answers our CAS we want to reply to
                // the client who asked us to add to the value, so we store a mapping
                // from the ID of this message to the name of the client,
                // their message ID we ought to respond to, and the delta to apply.
                self.request_map
                    .insert(self.msg_id, (msg.src, msg.body.id.unwrap(), Some(delta)));
                self.unacknowledged.push(kv_request);
                self.unacknowledged.last().unwrap().send(output).await?;
                self.msg_id += 1;
            }
            InnerMessageBody::CasKvOk => {
                let mut duplicate = true;
                // We received a response to our CAS request, so mark it as acknowledged.
                for (i, m) in self.unacknowledged.iter().enumerate() {
                    if m.body.id.unwrap() == msg.body.in_reply_to.unwrap() {
                        self.unacknowledged.remove(i);
                        duplicate = false;
                        break;
                    }
                }
                if !duplicate {
                    // Check if this was a CAS request issued in response to a client
                    // request or our initial request.
                    // If client request:
                    // Respond to the client that their Add request was succesful.
                    // First we retrieve the ID of the client we need to respond to
                    // as well as their request's message ID.
                    if let Some((client_id, in_reply_to, _)) =
                        self.request_map.remove(&msg.body.in_reply_to.unwrap())
                    {
                        let reply = Message {
                            src: self.id.as_ref().unwrap().to_string(),
                            dst: client_id,
                            body: MessageBody {
                                id: Some(self.msg_id),
                                in_reply_to: Some(in_reply_to),
                                inner: InnerMessageBody::AddOk,
                            },
                        };
                        reply.send(output).await?;
                        self.msg_id += 1;
                    }
                }
            }
            InnerMessageBody::WriteKvOk => {
                // Do we need writes for anything?
                todo!()
            }
            // Precondition failed, i.e., the CAS failed because the `from` value we tried was outdated.
            InnerMessageBody::Error {
                code: 22,
                text: Some(error),
            } => {
                let mut duplicate = true;
                // We received an error response to our CAS request, so mark it as acknowledged.
                for (i, m) in self.unacknowledged.iter().enumerate() {
                    if m.body.id.unwrap() == msg.body.in_reply_to.unwrap() {
                        self.unacknowledged.remove(i);
                        duplicate = false;
                        break;
                    }
                }
                if !duplicate {
                    // We _could_ send another Read request and _then_ try to issue a new CAS,
                    // but the error message contains the (hopefully) current value,
                    // so let's try to parse it from the message. :3
                    let parsed_value: u64 = error
                        .trim_start_matches(|c| !char::is_numeric(c))
                        .chars()
                        .take_while(|c| char::is_numeric(*c))
                        .collect::<String>()
                        .parse()
                        .context("in Error 22 match arm")
                        .expect("Failed to parse number");
                    let (client_id, in_reply_to, delta) = self
                        .request_map
                        .remove(&msg.body.in_reply_to.unwrap())
                        .context("in Error 22 match arm")
                        .expect("could not retrieve request mapping");
                    let new_value: u64 = parsed_value + delta.expect("delta should have been set");
                    let kv_request = Message {
                        src: self.id.as_ref().unwrap().to_string(),
                        dst: SEQ_KV.to_owned(),
                        body: MessageBody {
                            id: Some(self.msg_id),
                            in_reply_to: None,
                            // TODO: Do we store one single counter in the KV, or do we
                            // store one counter per node and later combine them?
                            inner: InnerMessageBody::CasKv {
                                key: COUNTER.to_owned(),
                                from: parsed_value.to_string(),
                                to: new_value.to_string(),
                                // Since we already read the value, the key should exist.
                                // There are no delete operations on the KV store.
                                // Additionally it is sequentially consistent, which implies monotonic reads.
                                create_if_not_exists: false,
                            },
                        },
                    };
                    // Once the KV store answers our CAS we want to reply to
                    // the client who asked us to add to the value, so we store a mapping
                    // from the ID of this message to the name of the client,
                    // their message ID we ought to respond to,
                    // and the delta to apply (in case the CAS fails and we need to repeat this dance).
                    self.request_map
                        .insert(self.msg_id, (client_id, in_reply_to, delta));
                    self.unacknowledged.push(kv_request);
                    self.unacknowledged.last().unwrap().send(output).await?;
                    self.msg_id += 1;
                }
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
