//! Long-running deterministic storage simulation.

mod oracle;
mod restart;
mod runner;
mod workload;

use std::{
    collections::VecDeque,
    fmt,
    process::ExitCode,
    sync::{Arc, Mutex, MutexGuard},
    thread,
    time::{Duration, Instant},
};

use crate::error::StorageError;
use runner::Simulation;

const HELP: &str = "Usage: storage-sim --seed N [--duration 15m] [--events N]\n\
    Durations accept seconds, minutes, or hours (such as 30s, 15m, 2h).\n\
    With --events alone, run without a wall-clock limit.\n";
const WATCHDOG_TIMEOUT: Duration = Duration::from_secs(30);
const TRACE_CAPACITY: usize = 36;

#[derive(Debug, thiserror::Error)]
enum RunError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("{0}")]
    Mismatch(String),
}

type RunResult<T> = Result<T, RunError>;

struct Config {
    seed: u64,
    duration: Option<Duration>,
    events: Option<u64>,
}

impl Config {
    fn parse(args: impl Iterator<Item = String>) -> Result<Option<Self>, String> {
        let mut seed = None;
        let mut duration = None;
        let mut events = None;
        let mut args = args;
        while let Some(flag) = args.next() {
            if flag == "--help" || flag == "-h" {
                return Ok(None);
            }
            let value = args.next().ok_or_else(|| format!("missing value for {flag}"))?;
            match flag.as_str() {
                "--seed" if seed.is_none() => seed = Some(number(&flag, &value)?),
                "--duration" if duration.is_none() => duration = Some(parse_duration(&value)?),
                "--events" if events.is_none() => events = Some(positive(&flag, &value)?),
                _ => return Err(format!("unknown or repeated option {flag}")),
            }
        }
        let seed = seed.ok_or_else(|| "--seed is required".to_owned())?;
        if duration.is_none() && events.is_none() {
            duration = Some(Duration::from_secs(15 * 60));
        }
        Ok(Some(Self { seed, duration, events }))
    }
}

fn number(flag: &str, value: &str) -> Result<u64, String> {
    value.parse().map_err(|error| format!("invalid {flag} value {value:?} ({error})"))
}

fn positive(flag: &str, value: &str) -> Result<u64, String> {
    let number = number(flag, value)?;
    if number == 0 {
        return Err(format!("{flag} must be positive"));
    }
    Ok(number)
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    for (suffix, multiplier) in [('s', 1), ('m', 60), ('h', 3_600)] {
        if let Some(count) = value.strip_suffix(suffix) {
            let seconds = positive("--duration", count)?
                .checked_mul(multiplier)
                .ok_or("--duration is too large")?;
            return Ok(Duration::from_secs(seconds));
        }
    }
    Err(format!("invalid --duration value {value:?}"))
}

struct Trace {
    seed: u64,
    state: Mutex<TraceState>,
}

struct TraceState {
    event: u64,
    tick: u64,
    last_progress: Instant,
    lines: VecDeque<String>,
}

impl Trace {
    fn new(seed: u64) -> Self {
        Self {
            seed,
            state: Mutex::new(TraceState {
                event: 0,
                tick: 0,
                last_progress: Instant::now(),
                lines: VecDeque::with_capacity(TRACE_CAPACITY),
            }),
        }
    }

    fn state(&self) -> MutexGuard<'_, TraceState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn event(&self, event: u64, tick: u64, message: fmt::Arguments<'_>) {
        let mut state = self.state();
        state.event = event;
        state.tick = tick;
        state.last_progress = Instant::now();
        state.push(format!("{event:>10} @{tick} {message}"));
    }

    fn note(&self, message: fmt::Arguments<'_>) {
        self.state().push(format!("             {message}"));
    }

    fn report(&self, state: &TraceState, error: fmt::Arguments<'_>) {
        eprintln!(
            "FAIL seed {} at event {}, virtual tick {}: {error}",
            self.seed, state.event, state.tick,
        );
        for line in &state.lines {
            eprintln!("{line}");
        }
        if state.event > 0 {
            eprintln!("Replay with storage-sim --seed {} --events {}", self.seed, state.event);
        }
    }
}

impl TraceState {
    fn push(&mut self, line: String) {
        if self.lines.len() == TRACE_CAPACITY {
            self.lines.pop_front();
        }
        self.lines.push_back(line);
    }
}

/// Run the storage simulator with command-line arguments.
pub fn run_cli() -> ExitCode {
    let config = match Config::parse(std::env::args().skip(1)) {
        Ok(Some(config)) => config,
        Ok(None) => {
            print!("{HELP}");
            return ExitCode::SUCCESS;
        }
        Err(error) => {
            eprintln!("{error}\n{HELP}");
            return ExitCode::from(2);
        }
    };

    let trace = Arc::new(Trace::new(config.seed));
    let panic_trace = Arc::clone(&trace);
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if let Ok(state) = panic_trace.state.try_lock() {
            panic_trace.report(&state, format_args!("engine panicked"));
        }
        previous_hook(info);
    }));

    let watchdog_trace = Arc::clone(&trace);
    let watchdog = thread::Builder::new().name("storage-sim watchdog".into()).spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(1));
            let state = watchdog_trace.state();
            if state.last_progress.elapsed() >= WATCHDOG_TIMEOUT {
                watchdog_trace
                    .report(&state, format_args!("no event progress for {WATCHDOG_TIMEOUT:?}"));
                std::process::exit(1);
            }
        }
    });
    if let Err(error) = watchdog {
        eprintln!("cannot start watchdog: {error}");
        return ExitCode::FAILURE;
    }

    println!(
        "Running seed {}, duration {:?}, event limit {:?}",
        config.seed, config.duration, config.events
    );
    match run(&config, &trace) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            trace.report(&trace.state(), format_args!("{error}"));
            ExitCode::FAILURE
        }
    }
}

fn run(config: &Config, trace: &Trace) -> RunResult<()> {
    let start = Instant::now();
    trace.event(0, 0, format_args!("create database and commit initial trees"));
    let mut simulation = Simulation::new(config.seed)?;
    let mut reported = Duration::ZERO;
    let mut events = 0;
    let reason = loop {
        if config.events.is_some_and(|limit| events >= limit) {
            break "event limit";
        }
        let elapsed = start.elapsed();
        if config.duration.is_some_and(|limit| elapsed >= limit) {
            break "time limit";
        }
        events += 1;
        simulation.step(events, trace)?;
        if elapsed.saturating_sub(reported) >= Duration::from_secs(60) {
            println!(
                "Progress after {:.1}s at event {events}: {}",
                elapsed.as_secs_f64(),
                simulation.summary()
            );
            reported = elapsed;
        }
    };
    println!(
        "PASS seed {} reached {reason} after {:.1}s and {events} events (virtual tick {})",
        config.seed,
        start.elapsed().as_secs_f64(),
        trace.state().tick,
    );
    println!("{}", simulation.summary());
    Ok(())
}
