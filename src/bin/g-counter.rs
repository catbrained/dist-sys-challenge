use core::panic;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use tokio::sync::oneshot::Sender;
use tokio::sync::Mutex;
use tokio::{io, task};
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
    let output = Rc::new(Mutex::new(FramedWrite::new(stdout, codec)));

    let node = Node {
        id: Mutex::new(None),
        msg_id: AtomicU64::new(1),
        callbacks: Mutex::new(HashMap::new()),
    };
    let node = Rc::new(node);

    let local = tokio::task::LocalSet::new();

    let main_loop = async {
        while let Some(line) = input.try_next().await? {
            let message: Message = serde_json::from_str(&line)?;
            if let Some(id) = message.body.in_reply_to {
                if let Some(tx) = node.callbacks.lock().await.remove(&id) {
                    let _ = tx.send(message);
                }
            } else {
                task::spawn_local(handle_msg(node.clone(), message, output.clone()));
            }
        }
        Ok(())
    };
    local.run_until(main_loop).await
}

struct Node {
    id: Mutex<Option<String>>,
    msg_id: AtomicU64,
    callbacks: Mutex<HashMap<u64, Sender<Message>>>,
}

async fn handle_msg(
    node: Rc<Node>,
    msg: Message,
    output: Rc<Mutex<FramedWrite<io::Stdout, LinesCodec>>>,
) -> Result<()> {
    // NOTE: I'm assuming that all messages we receive are actually intended for us
    // and thus we don't need to check the destination value matches our id.
    match msg.body.inner {
        InnerMessageBody::Init { node_id, .. } => {
            {
                let mut id = node.id.lock().await;
                if id.is_some() {
                    panic!("Received Init message, but we already have a node ID");
                } else {
                    *id = Some(node_id);
                }
            }
            {
                // Let's initialize the counter in the KV store.
                let init_kv = Message {
                    src: node.id.lock().await.as_ref().unwrap().to_string(),
                    dst: SEQ_KV.to_owned(),
                    body: MessageBody {
                        id: Some(node.msg_id.fetch_add(1, Ordering::SeqCst)),
                        in_reply_to: None,
                        inner: InnerMessageBody::CasKv {
                            key: COUNTER.to_owned(),
                            from: "0".to_owned(),
                            to: "0".to_owned(),
                            create_if_not_exists: true,
                        },
                    },
                };
                init_kv
                    .send_with_retry(&node.callbacks, output.clone())
                    .await?;
            }
            let reply = Message {
                src: msg.dst,
                dst: msg.src,
                body: MessageBody {
                    id: Some(node.msg_id.fetch_add(1, Ordering::SeqCst)),
                    in_reply_to: msg.body.id,
                    inner: InnerMessageBody::InitOk,
                },
            };
            reply.send(output).await?;
        }
        InnerMessageBody::Read => {
            // Reads from the sequentially consistent KV store might return stale values.
            // We can prevent this by first issuing a unique write, which prevents the KV
            // store from reordering our read in undesireable ways.
            // (Although, as far as I understand it, the store _could_ still reorder the read
            // without violating sequential consistency, but proving that this reordering is
            // legal would be prohibitively expensive so it doesn't try to reorder. In any case,
            // the Maelstrom provided seq-kv service seems to behave in this way so we make use of that.)
            let msg_id = node.msg_id.fetch_add(1, Ordering::SeqCst);
            let node_id = node.id.lock().await.as_ref().unwrap().to_string();
            let kv_request = Message {
                src: node_id.clone(),
                dst: SEQ_KV.to_owned(),
                body: MessageBody {
                    id: Some(msg_id),
                    in_reply_to: None,
                    inner: InnerMessageBody::WriteKv {
                        key: node_id,
                        value: msg_id.to_string(),
                    },
                },
            };
            let reply = kv_request
                .send_with_retry(&node.callbacks, output.clone())
                .await?
                .await??;
            debug_assert!(matches!(reply.body.inner, InnerMessageBody::WriteKvOk));
            let read_request = Message {
                src: node.id.lock().await.as_ref().unwrap().to_string(),
                dst: SEQ_KV.to_owned(),
                body: MessageBody {
                    id: Some(node.msg_id.fetch_add(1, Ordering::SeqCst)),
                    in_reply_to: None,
                    inner: InnerMessageBody::ReadKv {
                        key: COUNTER.to_owned(),
                    },
                },
            };
            let reply = read_request
                .send_with_retry(&node.callbacks, output.clone())
                .await?
                .await??;
            let InnerMessageBody::ReadOk(ReadOkVariants::Kv { value }) = reply.body.inner else {
                panic!("Unexpected response type");
            };
            let value: u64 = value.parse().expect("Failed to parse counter value");
            let reply = Message {
                src: node.id.lock().await.as_ref().unwrap().to_string(),
                dst: msg.src,
                body: MessageBody {
                    id: Some(node.msg_id.fetch_add(1, Ordering::SeqCst)),
                    in_reply_to: msg.body.id,
                    inner: InnerMessageBody::ReadOk(ReadOkVariants::Single { value }),
                },
            };
            reply.send(output).await?;
        }
        InnerMessageBody::Add { delta } => {
            // Apparently clients sometimes issue an Add request with a delta of 0,
            // so let's check for that and skip contacting the KV store for those requests.
            if delta == 0 {
                let reply = Message {
                    src: node.id.lock().await.as_ref().unwrap().to_string(),
                    dst: msg.src,
                    body: MessageBody {
                        id: Some(node.msg_id.fetch_add(1, Ordering::SeqCst)),
                        in_reply_to: msg.body.id,
                        inner: InnerMessageBody::AddOk,
                    },
                };
                reply.send(output).await?;
            } else {
                // To add to the counter we first need to get the current value
                // and then issue a CAS.
                // Contact the KV store to get the value of the counter
                let kv_request = Message {
                    src: node.id.lock().await.as_ref().unwrap().to_string(),
                    dst: SEQ_KV.to_owned(),
                    body: MessageBody {
                        id: Some(node.msg_id.fetch_add(1, Ordering::SeqCst)),
                        in_reply_to: None,
                        inner: InnerMessageBody::ReadKv {
                            key: COUNTER.to_owned(),
                        },
                    },
                };
                let reply = kv_request
                    .send_with_retry(&node.callbacks, output.clone())
                    .await?
                    .await??;
                let InnerMessageBody::ReadOk(ReadOkVariants::Kv { mut value }) = reply.body.inner
                else {
                    panic!("Received unexpected response");
                };
                // We got the (hopefully) current value. Issue a CAS to update it.
                loop {
                    let parsed_value: u64 = value.parse().expect("Could not parse counter value");
                    let cas = Message {
                        src: node.id.lock().await.as_ref().unwrap().to_string(),
                        dst: SEQ_KV.to_owned(),
                        body: MessageBody {
                            id: Some(node.msg_id.fetch_add(1, Ordering::SeqCst)),
                            in_reply_to: None,
                            inner: InnerMessageBody::CasKv {
                                key: COUNTER.to_owned(),
                                from: value.clone(),
                                to: (parsed_value + delta).to_string(),
                                create_if_not_exists: false,
                            },
                        },
                    };
                    let reply = cas
                        .send_with_retry(&node.callbacks, output.clone())
                        .await?
                        .await??;
                    match reply.body.inner {
                        InnerMessageBody::CasKvOk => {
                            // Our CAS was successful. Return the response to the client.
                            let reply = Message {
                                src: node.id.lock().await.as_ref().unwrap().to_string(),
                                dst: msg.src,
                                body: MessageBody {
                                    id: Some(node.msg_id.fetch_add(1, Ordering::SeqCst)),
                                    in_reply_to: msg.body.id,
                                    inner: InnerMessageBody::AddOk,
                                },
                            };
                            return reply.send(output.clone()).await;
                        }
                        InnerMessageBody::Error {
                            code: 22,
                            text: Some(error),
                        } => {
                            // The CAS failed because the counter value was changed by someone.
                            // The error message contains the (hopefully) current value. Parse it and try again.
                            value = error
                                .trim_start_matches(|c| !char::is_numeric(c))
                                .chars()
                                .take_while(|c| char::is_numeric(*c))
                                .collect::<String>()
                                .parse()
                                .context("in Error 22 match arm")
                                .expect("Failed to parse number");
                        }
                        _ => {
                            panic!("Unexpected response type");
                        }
                    }
                }
            }
        }
        _ => {
            // NOTE: Let's assume that everyone is behaving nicely and we don't get
            // any `InitOk`s or other messages that we don't expect. :)
            unreachable!("unexpected message type encountered")
        }
    }
    Ok(())
}
