use databas_storage::TreeCursor;
use proptest::prelude::*;

use super::{Model, new_storage, owned, pair, value};

#[derive(Clone, Debug)]
enum Position {
    BeforeFirst,
    At(Vec<u8>),
    Exhausted,
}

impl Position {
    fn from_key(key: Option<&Vec<u8>>) -> Self {
        key.map_or(Self::Exhausted, |key| Self::At(key.clone()))
    }

    fn current(&self, model: &Model) -> Option<super::Pair> {
        match self {
            Self::At(key) => model.get_key_value(key).map(pair),
            Self::BeforeFirst | Self::Exhausted => None,
        }
    }

    fn next(&self, model: &Model) -> Self {
        match self {
            Self::BeforeFirst => Self::from_key(model.keys().next()),
            Self::At(key) => Self::from_key(model.keys().find(|candidate| *candidate > key)),
            Self::Exhausted => Self::Exhausted,
        }
    }
}

#[derive(Clone, Debug)]
enum Command {
    First,
    Seek(Vec<u8>, bool),
    SeekExisting(u16, bool),
    Get(u16),
    Next,
    Current,
    CloneOther,
}

fn command() -> impl Strategy<Value = Command> {
    prop_oneof![
        Just(Command::First),
        (prop::collection::vec(any::<u8>(), 0..=16), any::<bool>())
            .prop_map(|(key, strict)| Command::Seek(key, strict)),
        (any::<u16>(), any::<bool>())
            .prop_map(|(index, strict)| Command::SeekExisting(index, strict)),
        any::<u16>().prop_map(Command::Get),
        Just(Command::Next),
        Just(Command::Current),
        Just(Command::CloneOther),
    ]
}

proptest! {
    #![proptest_config(super::config())]

    #[test]
    fn cursor_positions_and_clones_match_a_logical_scan(
        model in prop::collection::btree_map(prop::collection::vec(any::<u8>(), 0..=16), value(), 0..=40),
        commands in prop::collection::vec((any::<bool>(), command()), 1..=96),
    ) {
        let (_directory, storage) = new_storage(8)?;
        let mut builder = storage.create_tree()?;
        for (key, value) in &model {
            builder.insert(key, value)?;
        }
        let cursor = storage.tree_cursor(builder.root_page_id())?;
        let mut cursors = [cursor.clone(), cursor];
        let mut positions = [Position::BeforeFirst, Position::BeforeFirst];
        for (second, command) in commands {
            let index = usize::from(second);
            let command = match command {
                Command::SeekExisting(hint, strict) => {
                    let key = model.keys().nth(usize::from(hint) % model.len().max(1))
                        .cloned().unwrap_or_default();
                    Command::Seek(key, strict)
                }
                command => command,
            };
            match command {
                Command::First => {
                    positions[index] = Position::from_key(model.keys().next());
                    prop_assert_eq!(cursors[index].seek_to_first()?, !model.is_empty());
                }
                Command::Seek(probe, strict) => {
                    positions[index] = Position::from_key(model.keys().find(|key| {
                        if strict { *key > &probe } else { *key >= &probe }
                    }));
                    let found = if strict {
                        cursors[index].seek_after_key(&probe)?
                    } else {
                        cursors[index].seek_to_key(&probe)?
                    };
                    prop_assert_eq!(found, positions[index].current(&model).is_some());
                }
                Command::Get(hint) => {
                    let expected = model.iter().nth(usize::from(hint) % model.len().max(1));
                    let key = expected.map_or(&[][..], |(key, _)| key.as_slice());
                    prop_assert_eq!(owned(cursors[index].get(key)?)?, expected.map(pair));
                    positions[index] = expected.map_or(Position::BeforeFirst, |(key, _)| Position::At(key.clone()));
                }
                Command::Next => {
                    positions[index] = positions[index].next(&model);
                    prop_assert_eq!(owned(cursors[index].next_record()?)?, positions[index].current(&model));
                }
                Command::CloneOther => {
                    cursors[index] = cursors[1 - index].clone();
                    positions[index] = positions[1 - index].clone();
                }
                Command::Current | Command::SeekExisting(_, _) => {}
            }
            // Reading either cursor never moves it, and moving or cloning one
            // cursor must not move the other cursor's logical position.
            for index in 0..2 {
                prop_assert_eq!(owned(cursors[index].current()?)?, positions[index].current(&model));
            }
        }
    }
}

#[test]
#[allow(clippy::unwrap_used)]
fn reverse_cursor_crosses_leaf_boundaries_after_splits_and_merges() {
    let (_dir, storage) = super::new_storage(8).unwrap();
    let mut cursor = storage.create_tree().unwrap();
    assert!(!cursor.seek_to_last().unwrap());
    assert!(cursor.previous_record().unwrap().is_none());

    let payload = vec![0xab; 480];
    for key in (0_u16..200).step_by(2).rev() {
        cursor.insert(&key.to_be_bytes(), &payload).unwrap();
    }
    for key in (30_u16..170).step_by(2) {
        cursor.delete(&key.to_be_bytes()).unwrap();
    }

    let keys: Vec<_> = (0_u16..200).step_by(2).filter(|key| !(30..170).contains(key)).collect();
    let current_key = |cursor: &TreeCursor| {
        cursor.current().unwrap().map(|record| {
            record.with_key(|key| u16::from_be_bytes(key.try_into().unwrap())).unwrap()
        })
    };
    assert!(cursor.seek_to_last().unwrap());
    for &expected in keys.iter().rev() {
        assert_eq!(current_key(&cursor), Some(expected));
        cursor.previous_record().unwrap();
    }
    assert!(cursor.previous_record().unwrap().is_none());
    assert_eq!(current_key(&cursor), None);

    for probe in 0_u16..=201 {
        let bytes = probe.to_be_bytes();
        let expected = keys.iter().rev().find(|&&key| key <= probe).copied();
        assert_eq!(cursor.seek_to_key_at_most(&bytes).unwrap(), expected.is_some());
        assert_eq!(current_key(&cursor), expected);

        let expected = keys.iter().rev().find(|&&key| key < probe).copied();
        assert_eq!(cursor.seek_before_key(&bytes).unwrap(), expected.is_some());
        assert_eq!(current_key(&cursor), expected);
    }
}
