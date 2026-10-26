use crate::chunk::chunk_payload_data::ChunkPayloadData;

use alloc::collections::VecDeque;
use rustc_hash::FxHashMap;

/// pendingBaseQueue
pub(crate) type PendingBaseQueue = VecDeque<ChunkPayloadData>;

/// pendingQueue
#[derive(Debug, Default)]
pub(crate) struct PendingQueue {
    unordered_queue: PendingBaseQueue,
    ordered_queue: PendingBaseQueue,
    queue_len: usize,
    n_bytes: usize,
    /// User data bytes per stream identifier, for streams with any queued.
    n_bytes_by_stream: FxHashMap<u16, usize>,
    selected: bool,
    unordered_is_selected: bool,
}

impl PendingQueue {
    pub(crate) fn new() -> Self {
        PendingQueue::default()
    }

    pub(crate) fn push(&mut self, c: ChunkPayloadData) {
        self.n_bytes += c.user_data.len();
        *self
            .n_bytes_by_stream
            .entry(c.stream_identifier)
            .or_default() += c.user_data.len();
        if c.unordered {
            self.unordered_queue.push_back(c);
        } else {
            self.ordered_queue.push_back(c);
        }
        self.queue_len += 1;
    }

    pub(crate) fn peek(&self) -> Option<&ChunkPayloadData> {
        if self.selected {
            if self.unordered_is_selected {
                return self.unordered_queue.front();
            } else {
                return self.ordered_queue.front();
            }
        }

        let c = self.unordered_queue.front();

        if c.is_some() {
            return c;
        }

        self.ordered_queue.front()
    }

    pub(crate) fn pop(
        &mut self,
        beginning_fragment: bool,
        unordered: bool,
    ) -> Option<ChunkPayloadData> {
        let popped = if self.selected {
            let popped = if self.unordered_is_selected {
                self.unordered_queue.pop_front()
            } else {
                self.ordered_queue.pop_front()
            };
            if let Some(p) = &popped {
                if p.ending_fragment {
                    self.selected = false;
                }
            }
            popped
        } else {
            if !beginning_fragment {
                return None;
            }
            if unordered {
                let popped = { self.unordered_queue.pop_front() };
                if let Some(p) = &popped {
                    if !p.ending_fragment {
                        self.selected = true;
                        self.unordered_is_selected = true;
                    }
                }
                popped
            } else {
                let popped = { self.ordered_queue.pop_front() };
                if let Some(p) = &popped {
                    if !p.ending_fragment {
                        self.selected = true;
                        self.unordered_is_selected = false;
                    }
                }
                popped
            }
        };

        if let Some(p) = &popped {
            self.n_bytes -= p.user_data.len();
            self.queue_len -= 1;
            if let Some(n) = self.n_bytes_by_stream.get_mut(&p.stream_identifier) {
                *n -= p.user_data.len();
                if *n == 0 {
                    self.n_bytes_by_stream.remove(&p.stream_identifier);
                }
            }
        }

        popped
    }

    pub(crate) fn get_num_bytes(&self) -> usize {
        self.n_bytes
    }

    /// The user data bytes queued for one stream.
    pub(crate) fn get_num_bytes_for_stream(&self, stream_identifier: u16) -> usize {
        self.n_bytes_by_stream
            .get(&stream_identifier)
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn len(&self) -> usize {
        self.queue_len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn contains_stream(&self, stream_identifier: u16) -> bool {
        self.unordered_queue
            .iter()
            .chain(self.ordered_queue.iter())
            .any(|chunk| chunk.stream_identifier == stream_identifier)
    }
}
