use std::ops::Range;

#[derive(Debug)]
pub struct ItemHeader {
    key_len: u8,
    optional_len: u8,
    is_deleted: bool,
    is_numeric: bool,
    value_len: u32,
}

impl ItemHeader {
    /// Magic bytes to detect valid headers (0xCA 0xCE = "CACHE")
    pub const MAGIC0: u8 = 0xCA;
    pub const MAGIC1: u8 = 0xCE;

    /// Create a new ItemHeader
    pub fn new(
        key_len: u8,
        optional_len: u8,
        value_len: u32,
        is_deleted: bool,
        is_numeric: bool,
    ) -> Self {
        Self {
            key_len,
            optional_len,
            is_deleted,
            is_numeric,
            value_len,
        }
    }

    /// The packed length of the item header
    #[cfg(feature = "validation")]
    pub const SIZE: usize = 8; // With magic bytes and checksum

    #[cfg(not(feature = "validation"))]
    pub const SIZE: usize = 5; // Original compact format

    /// Maximum key length (8 bits)
    pub const MAX_KEY_LEN: usize = 0xFF;

    /// Maximum optional metadata length (6 bits in flags)
    pub const MAX_OPTIONAL_LEN: usize = 0x3F;

    /// Maximum value length (24 bits)
    pub const MAX_VALUE_LEN: usize = (1 << 24) - 1;

    /// Minimum valid item size for bounds checking
    pub const MIN_ITEM_SIZE: usize = Self::SIZE;

    /// Compute a simple checksum for validation (excluding is_deleted flag)
    /// Uses XOR of immutable fields only - is_deleted is mutable via atomic update
    fn compute_checksum(key_len: u8, optional_len: u8, value_len: u32, is_numeric: bool) -> u8 {
        let mut checksum = Self::MAGIC0;
        checksum ^= Self::MAGIC1;
        checksum ^= key_len;
        checksum ^= optional_len;
        checksum ^= (value_len & 0xFF) as u8;
        checksum ^= ((value_len >> 8) & 0xFF) as u8;
        checksum ^= ((value_len >> 16) & 0xFF) as u8;
        if is_numeric { checksum ^= 0x55; }
        checksum
    }

    /// Try to parse header from bytes, returning None if validation fails
    /// This is used in paths where invalid headers are expected (e.g., hashtable scanning)
    pub fn try_from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < ItemHeader::SIZE {
            return None;
        }

        // Parse header fields first
        let len = u32::from_ne_bytes([data[0], data[1], data[2], data[3]]);
        let flags = data[4];

        let key_len = len as u8;
        let value_len = len >> 8;
        let optional_len = flags & 0x3F;
        let is_deleted = (flags & 0x40) != 0;
        let is_numeric = (flags & 0x80) != 0;

        #[cfg(feature = "validation")]
        {
            // Validate magic bytes
            if data[5] != Self::MAGIC0 || data[6] != Self::MAGIC1 {
                return None;
            }

            // Validate checksum
            let stored_checksum = data[7];
            let computed_checksum = Self::compute_checksum(key_len, optional_len, value_len, is_numeric);
            if stored_checksum != computed_checksum {
                return None;
            }
        }

        Some(Self {
            key_len,
            optional_len,
            is_deleted,
            is_numeric,
            value_len,
        })
    }

    /// Parse header from bytes, panicking if validation fails
    /// This is used in paths where corruption is unexpected and indicates a serious bug
    pub fn from_bytes(data: &[u8]) -> Self {
        debug_assert!(data.len() >= ItemHeader::SIZE);

        // Parse header fields first
        // Layout: [len_low, len_mid, len_high, len_high, flags, MAGIC0, MAGIC1, checksum]
        // This keeps the old 5-byte layout in bytes 0-4, adds 3 bytes for validation
        let len = u32::from_ne_bytes([data[0], data[1], data[2], data[3]]);
        let flags = data[4];

        let key_len = len as u8;
        let value_len = len >> 8;
        let optional_len = flags & 0x3F;
        let is_deleted = (flags & 0x40) != 0;
        let is_numeric = (flags & 0x80) != 0;

        #[cfg(feature = "validation")]
        {
            // Validate magic bytes (at end of header to not interfere with atomic flag updates)
            assert!(data[5] == Self::MAGIC0 && data[6] == Self::MAGIC1,
                "CORRUPTION: Invalid magic bytes in item header: expected [0xCA, 0xCE] at bytes [5,6], got [0x{:02X}, 0x{:02X}]",
                data[5], data[6]);

            // Validate checksum (excludes is_deleted since it's mutable)
            let stored_checksum = data[7];
            let computed_checksum = Self::compute_checksum(key_len, optional_len, value_len, is_numeric);
            assert!(stored_checksum == computed_checksum,
                "CORRUPTION: Checksum mismatch in item header: expected 0x{:02X}, got 0x{:02X}. \
                 Data: key_len={}, optional_len={}, value_len={}, is_numeric={}",
                computed_checksum, stored_checksum, key_len, optional_len, value_len, is_numeric);
        }

        Self {
            key_len,
            optional_len,
            is_deleted,
            is_numeric,
            value_len,
        }
    }

    pub fn to_bytes(&self, data: &mut [u8]) {
        debug_assert!(data.len() >= ItemHeader::SIZE);

        // Write original 5-byte format in bytes 0-4 (compatible with atomic flag updates)
        let len = (self.value_len << 8) | (self.key_len as u32);
        let mut flags = self.optional_len;

        if self.is_deleted {
            flags |= 0x40;
        }

        if self.is_numeric {
            flags |= 0x80;
        }

        data[0..4].copy_from_slice(&len.to_ne_bytes());
        data[4] = flags;

        #[cfg(feature = "validation")]
        {
            // Write magic bytes and checksum at end (bytes 5-7)
            data[5] = Self::MAGIC0;
            data[6] = Self::MAGIC1;
            data[7] = Self::compute_checksum(self.key_len, self.optional_len, self.value_len, self.is_numeric);
        }
    }

    pub fn key_range(&self, offset: u32) -> Range<usize> {
        let offset = offset as usize;

        let start = offset + Self::SIZE + self.optional_len as usize;

        start..(start + self.key_len as usize)
    }

    pub fn value_range(&self, offset: u32) -> Range<usize> {
        let offset = offset as usize;

        let start = offset + Self::SIZE + self.optional_len as usize + self.key_len as usize;

        start..(start + self.value_len as usize)
    }

    pub fn optional_range(&self, offset: u32) -> Range<usize> {
        let offset = offset as usize;

        let start = offset + Self::SIZE;

        start..(start + self.optional_len as usize)
    }

    /// Calculate the padded size for this item, rounded to 8-byte boundary
    pub fn padded_size(&self) -> usize {
        let size = Self::SIZE
            .checked_add(self.optional_len as usize)
            .and_then(|s| s.checked_add(self.key_len as usize))
            .and_then(|s| s.checked_add(self.value_len as usize))
            .and_then(|s| s.checked_add(7))  // Add 7 for 8-byte alignment
            .expect("Item size overflow");

        size & !7  // Round up to 8-byte boundary
    }

    // Getter methods for accessing private fields
    pub fn is_deleted(&self) -> bool {
        self.is_deleted
    }

    pub fn key_len(&self) -> u8 {
        self.key_len
    }

    pub fn optional_len(&self) -> u8 {
        self.optional_len
    }

    pub fn value_len(&self) -> u32 {
        self.value_len
    }

    pub fn is_numeric(&self) -> bool {
        self.is_numeric
    }
}

#[derive(Debug)]
pub struct Item<'a> {
    header: ItemHeader,
    raw: &'a [u8],
}

impl<'a> Item<'a> {
    /// Create a new Item
    pub fn new(header: ItemHeader, raw: &'a [u8]) -> Self {
        Self { header, raw }
    }

    pub fn key(&self) -> &[u8] {
        &self.raw[self.header.key_range(0)]
    }

    pub fn value(&self) -> &[u8] {
        &self.raw[self.header.value_range(0)]
    }

    pub fn optional(&self) -> &[u8] {
        &self.raw[self.header.optional_range(0)]
    }

    /// Convert the value slice into an owned Vec, avoiding unnecessary copies
    /// This consumes the buffer that was passed to get() and extracts just the value
    pub fn value_to_vec(&self) -> Vec<u8> {
        self.value().to_vec()
    }
}

/// A guard that holds a reference to an item's data in a segment.
///
/// This is a zero-copy, zero-allocation view into the segment's data.
/// The segment's reference count is held while this guard exists,
/// preventing eviction or clearing of the segment.
///
/// The guard is dropped automatically when it goes out of scope, at which
/// point the segment's reference count is decremented.
///
/// # Examples
///
/// ```ignore
/// let guard = cache.get(b"key")?;
/// // Zero-copy access to item data
/// let value = guard.value();
/// // Can serialize directly to socket, etc.
/// socket.write_all(value)?;
/// // Guard dropped here, ref_count decremented
/// ```
pub struct ItemGuard<'a> {
    segment: &'a crate::segments::Segment<'a>,
    key: &'a [u8],
    value: &'a [u8],
    optional: &'a [u8],
}

impl<'a> ItemGuard<'a> {
    /// Create a new ItemGuard
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - The segment's ref_count has been incremented
    /// - The slice references are valid and point into the segment's data
    pub(crate) fn new(
        segment: &'a crate::segments::Segment<'a>,
        key: &'a [u8],
        value: &'a [u8],
        optional: &'a [u8],
    ) -> Self {
        Self {
            segment,
            key,
            value,
            optional,
        }
    }

    /// Get the item's key
    pub fn key(&self) -> &[u8] {
        self.key
    }

    /// Get the item's value
    pub fn value(&self) -> &[u8] {
        self.value
    }

    /// Get the item's optional metadata
    pub fn optional(&self) -> &[u8] {
        self.optional
    }
}

impl Drop for ItemGuard<'_> {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering;
        self.segment.ref_count.fetch_sub(1, Ordering::Release);
    }
}
