use anyhow::{bail, Error};

/// Helper to store data, accessible by integer ID
///
/// Lookup data by ID is very fast, so this can be used to
/// generate integer handles to pass them to "C" code.
pub struct Registry<T> {
    info_list: Vec<T>,
}

impl<T> Registry<T> {
    /// Create a new instance
    pub fn new() -> Self {
        Self {
            info_list: Vec::new(),
        }
    }

    /// Register data, returns associated ID
    pub fn register(&mut self, info: T) -> Result<u8, Error> {
        let dev_id = self.info_list.len();
        if dev_id > 255 {
            bail!("register failed - too many images/archives (limit is 255)");
        }
        self.info_list.push(info);
        Ok(dev_id as u8)
    }

    /// Lookup previously registered data by ID
    pub fn lookup(&mut self, id: u8) -> Result<&mut T, Error> {
        if id as usize >= self.info_list.len() {
            bail!("lookup failed for id = {}", id);
        }
        Ok(&mut self.info_list[id as usize])
    }
}
