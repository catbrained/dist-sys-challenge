use std::{
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::Result;
use tokio::{io, sync::Mutex, task};
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
    };
    let node = Rc::new(node);

    let local = tokio::task::LocalSet::new();

    let main_loop = async {
        while let Some(line) = input.try_next().await? {
            let message: Message = serde_json::from_str(&line)?;
            task::spawn_local(handle_msg(node.clone(), message, output.clone()));
        }
        Ok(())
    };
    local.run_until(main_loop).await
}

struct Node {
    id: Mutex<Option<String>>,
    msg_id: AtomicU64,
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
            // TODO: the node IDs sent in the init message are ignored for now,
            // since we're not using them for anything at the moment.
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
        InnerMessageBody::Generate => {
            let msg_id = node.msg_id.fetch_add(1, Ordering::SeqCst);
            let reply = Message {
                src: msg.dst,
                dst: msg.src,
                body: MessageBody {
                    id: Some(msg_id),
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
                        id: format!("{}-{}", node.id.lock().await.as_ref().unwrap(), msg_id),
                    },
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
