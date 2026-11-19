use std::collections::BTreeMap;

use crate::{TreeCursor, TxnId};

pub const CLIENTS: usize = 8;
pub const TREES: usize = 6;
pub const KEYS: u64 = 1_024;

pub type Contents = BTreeMap<Vec<u8>, Vec<u8>>;
pub type Changes = BTreeMap<(usize, Vec<u8>), Option<Vec<u8>>>;

pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// SplitMix64 keeps seeded runs independent of library RNG versions.
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut word = self.0;
        word = (word ^ (word >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        word = (word ^ (word >> 27)).wrapping_mul(0x94d049bb133111eb);
        word ^ (word >> 31)
    }

    pub fn below(&mut self, upper: u64) -> u64 {
        self.next() % upper
    }

    #[allow(clippy::cast_possible_truncation)] // The remainder fits in the original bound.
    pub fn index(&mut self, upper: usize) -> usize {
        self.below(upper as u64) as usize
    }

    pub fn chance(&mut self, denominator: u64) -> bool {
        self.below(denominator) == 0
    }
}

pub struct Client {
    pub ready: u64,
    pub rng: Rng,
    pub transaction: Option<Transaction>,
}

impl Client {
    pub fn new(seed: u64) -> Self {
        let mut rng = Rng::new(seed);
        Self { ready: rng.below(100), rng, transaction: None }
    }

    pub fn delay(&mut self, command: &Command) -> u64 {
        match command {
            Command::Commit | Command::Rollback if self.rng.chance(12) => 150 + self.rng.below(500),
            Command::Get { .. } | Command::Set { .. } | Command::Delete { .. }
                if self.rng.chance(3) =>
            {
                1 + self.rng.below(8)
            }
            _ => 2 + self.rng.below(68),
        }
    }

    pub fn plan(&mut self, index: usize) -> Command {
        let rng = &mut self.rng;
        let Some(transaction) = &self.transaction else {
            let first = tree_choice(rng, index);
            let second = rng.chance(10).then(|| tree_choice(rng, index));
            let mut trees = vec![first];
            if let Some(second) = second.filter(|second| *second != first) {
                trees.push(second);
                trees.sort_unstable();
            }
            let remaining = if rng.chance(64) { 24 + rng.below(72) } else { 1 + rng.below(8) };
            return Command::Begin { trees, remaining, read_heavy: rng.chance(3) };
        };
        if transaction.remaining == 0 {
            return if rng.chance(12) { Command::Rollback } else { Command::Commit };
        }

        let tree = transaction.cursors[rng.index(transaction.cursors.len())].0;
        let key = if rng.chance(3) { rng.below(KEYS) } else { rng.below(96) };
        let command = rng.below(100);
        if command < if transaction.read_heavy { 94 } else { 44 } {
            Command::Get { tree, key }
        } else if command < 61 {
            Command::Delete { tree, key }
        } else {
            let len = match rng.below(100) {
                0 => 4_096 + rng.index(12_288),
                1..=10 => 512 + rng.index(1_536),
                11..=14 => rng.index(8),
                _ => 16 + rng.index(272),
            };
            Command::Set { tree, key, len, token: rng.next() }
        }
    }
}

fn tree_choice(rng: &mut Rng, client: usize) -> usize {
    match rng.below(5) {
        0 => 0,
        1 | 2 => 1 + client % (TREES - 1),
        _ => rng.index(TREES),
    }
}

pub struct Transaction {
    pub id: TxnId,
    pub cursors: Vec<(usize, TreeCursor)>,
    pub writes: Changes,
    pub remaining: u64,
    pub read_heavy: bool,
}

impl Transaction {
    pub fn uses(&self, tree: usize) -> bool {
        self.cursors.iter().any(|(id, _)| *id == tree)
    }
}

#[derive(Debug)]
pub enum Command {
    Begin { trees: Vec<usize>, remaining: u64, read_heavy: bool },
    Get { tree: usize, key: u64 },
    Set { tree: usize, key: u64, len: usize, token: u64 },
    Delete { tree: usize, key: u64 },
    Commit,
    Rollback,
}

pub fn key(number: u64) -> Vec<u8> {
    let mut key = number.to_be_bytes().to_vec();
    if number % 251 == 250 {
        key.resize(4_202, 0x5a);
    }
    key
}

pub fn value(len: usize, token: u64) -> Vec<u8> {
    let mut value = vec![0; len];
    let mut rng = Rng::new(token);
    for chunk in value.chunks_mut(8) {
        chunk.copy_from_slice(&rng.next().to_le_bytes()[..chunk.len()]);
    }
    value
}
