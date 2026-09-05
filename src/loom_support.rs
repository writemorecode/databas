//! Shared configuration for bounded Loom models.

use std::sync::Arc as StdArc;

use loom::model::Builder;

pub(crate) mod thread {
    pub(crate) use loom::thread::yield_now;

    const STACK_SIZE: usize = 256 * 1024;

    pub(crate) fn spawn<T>(
        function: impl FnOnce() -> T + Send + 'static,
    ) -> loom::thread::JoinHandle<T>
    where
        T: Send + 'static,
    {
        loom::thread::Builder::new().stack_size(STACK_SIZE).spawn(function).unwrap()
    }
}

/// Explores a focused two-thread race with bounded scheduling choices.
pub(crate) fn check_model(model: impl Fn() + Send + Sync + 'static) {
    let model = StdArc::new(model);
    let mut builder = Builder::new();
    builder.preemption_bound = Some(2);
    builder.max_branches = 200;
    builder.check(move || {
        let model = StdArc::clone(&model);
        thread::spawn(move || model()).join().unwrap();
    });
}
