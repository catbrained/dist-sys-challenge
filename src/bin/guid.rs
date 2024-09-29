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
            InnerMessageBody::Generate => {
                let reply = Message {
                    src: msg.dst,
                    dst: msg.src,
                    body: MessageBody {
                        id: Some(self.msg_id),
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
                            id: format!("{}-{}", self.id.as_ref().unwrap(), self.msg_id),
                        },
                    },
                };
                reply.send(&mut *output).unwrap();
                self.msg_id += 1;
            }
            _ => {
                // NOTE: Let's assume that everyone is behaving nicely and we don't get
                // any `InitOk`s or other messages that we don't expect. :)
                unreachable!()
            }
        }
    }
}
