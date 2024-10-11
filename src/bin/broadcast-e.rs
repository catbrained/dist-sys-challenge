use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use tokio::sync::oneshot::Sender;
use tokio::sync::Mutex;
use tokio::time::{self, Duration, Instant, MissedTickBehavior};
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
        known: Mutex::new(HashSet::new()),
        neighbors: Mutex::new(Vec::new()),
        nodes: Mutex::new(Vec::new()),
        callbacks: Mutex::new(HashMap::new()),
        next_batch: Mutex::new(HashMap::new()),
    };
    let node = Rc::new(node);

    let local = tokio::task::LocalSet::new();

    let start = Instant::now() + Duration::from_millis(150);
    let mut batch_interval = time::interval_at(start, Duration::from_millis(150));
    batch_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    local
        .run_until(async {
            loop {
                tokio::select! {
                    Ok(l) = input.try_next() => {
                        let Some(line) = l else {
                            panic!("No more lines");
                        };
                        let message: Message = serde_json::from_str(&line)?;
                        if let Some(id) = message.body.in_reply_to {
                            if let Some(tx) = node.callbacks.lock().await.remove(&id) {
                                let _ = tx.send(message);
                            }
                        } else {
                            task::spawn_local(handle_msg(node.clone(), message, output.clone()));
                        }
                    }
                    _ = batch_interval.tick() => {
                        for (k, v) in node.next_batch.lock().await.iter_mut() {
                            let gossip = Message {
                                src: node.id.lock().await.as_ref().unwrap().clone(),
                                dst: k.clone(),
                                body: MessageBody {
                                    id: Some(node.msg_id.fetch_add(1, Ordering::SeqCst)),
                                    in_reply_to: None,
                                    inner: InnerMessageBody::BatchBroadcast {
                                        messages: std::mem::take(v),
                                    },
                                },
                            };
                            gossip.send_with_retry(&node.callbacks, output.clone()).await?;
                        }
                    }
                }
            }
        })
        .await
}

struct Node {
    id: Mutex<Option<String>>,
    msg_id: AtomicU64,
    known: Mutex<HashSet<u64>>,
    neighbors: Mutex<Vec<String>>,
    nodes: Mutex<Vec<String>>,
    callbacks: Mutex<HashMap<u64, Sender<Message>>>,
    next_batch: Mutex<HashMap<String, Vec<u64>>>,
}

async fn handle_msg(
    node: Rc<Node>,
    msg: Message,
    output: Rc<Mutex<FramedWrite<io::Stdout, LinesCodec>>>,
) -> Result<()> {
    // NOTE: I'm assuming that all messages we receive are actually intended for us
    // and thus we don't need to check the destination value matches our id.
    match msg.body.inner {
        InnerMessageBody::Init {
            node_id,
            mut node_ids,
        } => {
            {
                let mut id = node.id.lock().await;
                if id.is_some() {
                    panic!("Received Init message, but we already have a node ID");
                } else {
                    *id = Some(node_id);
                }
            }
            node_ids.sort();
            *node.nodes.lock().await = node_ids;
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
        InnerMessageBody::Broadcast { message } => {
            // Mark value as seen. Only clients should send us a regular Broadcast message,
            // and we assume that clients don't send duplicates.
            let already_seen = !node.known.lock().await.insert(message);
            debug_assert!(!already_seen);
            // Add value to next batch, which will be sent asynchronously on a regular interval.
            // Only clients should send us regular Broadcast messages,
            // therefore we don't have to skip any of our neighbors for rebroadcast.
            for n in node.neighbors.lock().await.iter() {
                node.next_batch
                    .lock()
                    .await
                    .entry(n.clone())
                    .and_modify(|e| e.push(message))
                    .or_insert(vec![message]);
            }
            // NOTE: should we delay the BroadcastOk until we've _actually_ broadcasted
            // the value to our neighbors? If so, this part should move to the batch send task.
            let reply = Message {
                src: msg.dst,
                dst: msg.src,
                body: MessageBody {
                    id: Some(node.msg_id.fetch_add(1, Ordering::SeqCst)),
                    in_reply_to: msg.body.id,
                    inner: InnerMessageBody::BroadcastOk,
                },
            };
            reply.send(output).await?;
        }
        InnerMessageBody::BatchBroadcast { messages } => {
            for message in messages {
                let already_seen = !node.known.lock().await.insert(message);
                // New values should be added to the next broadcast batch.
                if !already_seen {
                    for n in node.neighbors.lock().await.iter() {
                        if *n == msg.src {
                            // We don't need to send this value back to the node we got it from.
                            continue;
                        }
                        node.next_batch
                            .lock()
                            .await
                            .entry(n.clone())
                            .and_modify(|e| e.push(message))
                            .or_insert(vec![message]);
                    }
                }
            }
            // NOTE: should we delay the BroadcastOk until we've _actually_ broadcasted
            // the value to our neighbors? If so, this part should move to the batch send task.
            let reply = Message {
                src: msg.dst,
                dst: msg.src,
                body: MessageBody {
                    id: Some(node.msg_id.fetch_add(1, Ordering::SeqCst)),
                    in_reply_to: msg.body.id,
                    inner: InnerMessageBody::BroadcastOk,
                },
            };
            reply.send(output).await?;
        }
        InnerMessageBody::Topology { .. } => {
            // We ignore the topology suggestion from Maelstrom and build our own.
            // We build a tree with a maximum of `fanout` children per node.
            let fanout = 4;
            let nodes = node.nodes.lock().await;
            let i = nodes
                .binary_search(node.id.lock().await.as_ref().unwrap())
                .unwrap();
            let mut children: &[String] = &[];
            let child_idx = fanout * i + 1;
            // Check that we are not a leaf node.
            if child_idx < nodes.len() {
                children = &nodes[child_idx..(child_idx + fanout).min(nodes.len())];
            }
            let mut parent: &[String] = &[];
            if i != 0 {
                // We are not the root node, so get our parent.
                let parent_idx = (i - 1) / fanout;
                parent = &nodes[parent_idx..(parent_idx + 1)];
            }
            *node.neighbors.lock().await = [parent, children].concat();
            debug_assert!(node.neighbors.lock().await.len() <= fanout + 1);
            drop(nodes);
            let reply = Message {
                src: msg.dst,
                dst: msg.src,
                body: MessageBody {
                    id: Some(node.msg_id.fetch_add(1, Ordering::SeqCst)),
                    in_reply_to: msg.body.id,
                    inner: InnerMessageBody::TopologyOk,
                },
            };
            reply.send(output).await?;
        }
        InnerMessageBody::Read => {
            let reply = Message {
                src: msg.dst,
                dst: msg.src,
                body: MessageBody {
                    id: Some(node.msg_id.fetch_add(1, Ordering::SeqCst)),
                    in_reply_to: msg.body.id,
                    inner: InnerMessageBody::ReadOk(ReadOkVariants::Array {
                        messages: node.known.lock().await.clone().into_iter().collect(),
                    }),
                },
            };
            reply.send(output).await?;
        }
        _ => {
            // NOTE: Let's assume that everyone is behaving nicely and we don't get
            // any `InitOk`s or other messages that we don't expect. :)
            unreachable!()
        }
    }

    Ok(())
}
