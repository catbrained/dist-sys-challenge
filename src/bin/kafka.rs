use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use tokio::sync::oneshot::Sender;
use tokio::sync::Mutex;
use tokio::{io, task};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

use dist_sys_challenge::*;

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
        logs: Mutex::new(HashMap::new()),
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

#[derive(Debug, Default)]
struct Log {
    next_offset: u64,
    committed: u64,
    messages: Vec<(u64, u64)>,
}

struct Node {
    id: Mutex<Option<String>>,
    msg_id: AtomicU64,
    callbacks: Mutex<HashMap<u64, Sender<Message>>>,
    logs: Mutex<HashMap<String, Log>>,
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
        InnerMessageBody::Send { key, msg: m } => {
            let offset;
            {
                let mut logs = node.logs.lock().await;
                let l = logs.entry(key).or_default();
                l.messages.push((l.next_offset, m));
                offset = l.next_offset;
                l.next_offset += 1;
            }
            let reply = Message {
                src: node.id.lock().await.as_ref().unwrap().clone(),
                dst: msg.src,
                body: MessageBody {
                    id: Some(node.msg_id.fetch_add(1, Ordering::SeqCst)),
                    in_reply_to: msg.body.id,
                    inner: InnerMessageBody::SendOk { offset },
                },
            };
            reply.send(output).await?;
        }
        InnerMessageBody::Poll { offsets } => {
            let mut msgs = HashMap::new();
            {
                let logs = node.logs.lock().await;
                for (k, o) in offsets {
                    // Clients sometimes poll for logs that we don't know about.
                    if let Some(log) = logs.get(&k) {
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
            }
            let reply = Message {
                src: node.id.lock().await.as_ref().unwrap().clone(),
                dst: msg.src,
                body: MessageBody {
                    id: Some(node.msg_id.fetch_add(1, Ordering::SeqCst)),
                    in_reply_to: msg.body.id,
                    inner: InnerMessageBody::PollOk { msgs },
                },
            };
            reply.send(output).await?;
        }
        InnerMessageBody::CommitOffsets { offsets } => {
            {
                let mut logs = node.logs.lock().await;
                for (k, o) in offsets {
                    logs.entry(k).and_modify(|l| l.committed = o);
                }
            }
            let reply = Message {
                src: node.id.lock().await.as_ref().unwrap().clone(),
                dst: msg.src,
                body: MessageBody {
                    id: Some(node.msg_id.fetch_add(1, Ordering::SeqCst)),
                    in_reply_to: msg.body.id,
                    inner: InnerMessageBody::CommitOffsetsOk,
                },
            };
            reply.send(output).await?;
        }
        InnerMessageBody::ListCommittedOffsets { keys } => {
            let mut offsets = HashMap::new();
            {
                let logs = node.logs.lock().await;
                for k in keys {
                    if let Some(log) = logs.get(&k) {
                        offsets.insert(k, log.committed);
                    }
                }
            }
            let reply = Message {
                src: node.id.lock().await.as_ref().unwrap().clone(),
                dst: msg.src,
                body: MessageBody {
                    id: Some(node.msg_id.fetch_add(1, Ordering::SeqCst)),
                    in_reply_to: msg.body.id,
                    inner: InnerMessageBody::ListCommittedOffsetsOk { offsets },
                },
            };
            reply.send(output).await?;
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
