use disk_ringbuffer::ringbuf::Ringbuf;
use std::path::PathBuf;

pub struct Franz {
    ring: Ringbuf,
}

impl Franz {
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Franz {
            ring: Ringbuf::new(path),
        }
    }

    pub fn pop(&mut self) -> Option<String> {
        self.ring.pop()
    }

    pub fn push<T: AsRef<[u8]>>(&mut self, input: T) {
        self.ring.push(input)
    }
}
