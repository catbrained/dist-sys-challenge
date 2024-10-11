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
        InnerMessageBody::Echo { echo } => {
            let reply = Message {
                src: msg.dst,
                dst: msg.src,
                body: MessageBody {
                    id: Some(node.msg_id.fetch_add(1, Ordering::SeqCst)),
                    in_reply_to: msg.body.id,
                    inner: InnerMessageBody::EchoOk { echo },
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
