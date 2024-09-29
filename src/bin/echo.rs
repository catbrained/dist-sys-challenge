use std::io::{self, Write};

use dist_sys_challenge::*;

fn main() {
    let stdin = io::stdin().lock();
    let mut stdout = io::stdout().lock();

    let mut node = Node {
        id: None,
        msg_id: 1,
    };
    let messages = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
    for message in messages {
        // TODO: maybe we should replace a bunch of these `unwrap`s with proper error handling
        let message = message.unwrap();
        node.handle_msg(message, &mut stdout);
    }
}

struct Node {
    id: Option<String>,
    msg_id: u64,
}

impl Node {
    fn handle_msg(&mut self, msg: Message, output: &mut impl Write) {
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
                reply.send(&mut *output).unwrap();
                self.msg_id += 1;
            }
            InnerMessageBody::Echo { echo } => {
                let reply = Message {
                    src: msg.dst,
                    dst: msg.src,
                    body: MessageBody {
                        id: Some(self.msg_id),
                        in_reply_to: msg.body.id,
                        inner: InnerMessageBody::EchoOk { echo },
                    },
                };
                reply.send(&mut *output).unwrap();
                self.msg_id += 1;
            }
            _ => {
                // NOTE: Let's assume that everyone is behaving nicely and we don't get
                // any `InitOk`s or other messages that we don't expect. :)
                unimplemented!()
            }
        }
    }
}
