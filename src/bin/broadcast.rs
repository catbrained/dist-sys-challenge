use std::{
    collections::HashSet,
    io::{self, Write},
};

use dist_sys_challenge::*;

fn main() {
    let stdin = io::stdin().lock();
    let mut stdout = io::stdout().lock();

    let mut node = Node {
        id: None,
        msg_id: 1,
        known: HashSet::new(),
        neighbors: Vec::new(),
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
    known: HashSet<u64>,
    neighbors: Vec<String>,
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
            InnerMessageBody::Broadcast { message } => {
                let already_seen = !self.known.insert(message);
                // Is this a node-to-node message?
                let node_to_node = msg.src.starts_with("n");
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
                        gossip.send(&mut *output).unwrap();
                        self.msg_id += 1;
                    }
                }
                // We don't acknowledge node-to-node gossip.
                if !node_to_node {
                    let reply = Message {
                        src: msg.dst,
                        dst: msg.src,
                        body: MessageBody {
                            id: Some(self.msg_id),
                            in_reply_to: msg.body.id,
                            inner: InnerMessageBody::BroadcastOk,
                        },
                    };
                    reply.send(&mut *output).unwrap();
                    self.msg_id += 1;
                }
            }
            InnerMessageBody::Topology { topology } => {
                self.neighbors = topology.get(self.id.as_ref().unwrap()).unwrap().clone();
                let reply = Message {
                    src: msg.dst,
                    dst: msg.src,
                    body: MessageBody {
                        id: Some(self.msg_id),
                        in_reply_to: msg.body.id,
                        inner: InnerMessageBody::TopologyOk,
                    },
                };
                reply.send(&mut *output).unwrap();
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
