use slab::Slab;

pub const BUFFER_POOL_ITEM_SIZE: usize = 2048;

pub struct BufferPool {
    pool: Vec<usize>,
    alloc: Slab<Box<[u8]>>,
}

impl BufferPool {
    pub fn new(inital_capacity: usize) -> Self {
        Self {
            pool: Vec::with_capacity(inital_capacity),
            alloc: Slab::with_capacity(inital_capacity),
        }
    }
    pub fn get_mut<'a>(&'a mut self, index: usize) -> &'a mut [u8] {
        &mut self.alloc[index]
    }

    pub fn get<'a>(&'a self, index: usize) -> &'a [u8] {
        &self.alloc[index]
    }

    pub unsafe fn get_unchecked<'a>(&'a self, index: usize) -> &'a [u8] {
        unsafe { &self.alloc.get_unchecked(index) }
    }

    pub fn reuse_or_allocate(&mut self) -> (usize, &mut Box<[u8]>) {
        match self.pool.pop() {
            Some(index) => (index, &mut self.alloc[index]),
            None => {
                let buf = vec![0u8; BUFFER_POOL_ITEM_SIZE].into_boxed_slice();
                let entry = self.alloc.vacant_entry();
                let index = entry.key();
                (index, entry.insert(buf))
            }
        }
    }

    #[inline]
    pub fn return_to_pool(&mut self, index: usize) {
        self.pool.push(index);
    }
}
