//! Implementation of the measurement pipeline.

use std::collections::HashMap;
use std::future::Future;
use std::ops::BitOrAssign;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{anyhow, Context};

use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::{runtime::Runtime, sync::watch};

use crate::metrics::{Metric, RawMetricId};
use crate::pipeline::scoped;
use crate::{
    measurement::MeasurementBuffer,
    metrics::MetricRegistry,
    pipeline::{Output, Source, Transform},
};

use super::{builder, SourceType};
use super::trigger::{ConfiguredSourceTrigger, Trigger};
use super::{OutputContext, PollError, TransformError, WriteError};

pub struct MeasurementPipeline {
    // Elements of the pipeline
    pub(super) sources: Vec<builder::ConfiguredSource>,
    pub(super) transforms: Vec<builder::ConfiguredTransform>,
    pub(super) outputs: Vec<builder::ConfiguredOutput>,
    pub(super) autonomous_sources: Vec<Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>>,

    // tokio Runtimes that execute the tasks
    pub(super) rt_normal: Runtime,
    pub(super) rt_priority: Option<Runtime>,

    // registries
    pub(super) metrics: MetricRegistry,

    /// Channel: source -> transforms
    pub(super) from_sources: (mpsc::Sender<MeasurementBuffer>, mpsc::Receiver<MeasurementBuffer>),

    /// Broadcast queue to outputs
    pub(super) to_outputs: broadcast::Sender<OutputMsg>,
}

/// A message for an element of the pipeline.
enum ControlMessage {
    Source(Option<String>, SourceCmd),
    Output(Option<String>, OutputCmd),
    Transform(Option<String>, TransformCmd),
}
pub struct RunningPipeline {
    // Keep the tokio runtimes alive
    _rt_normal: Runtime,
    _rt_priority: Option<Runtime>,

    // Handles to wait for pipeline elements to finish.
    source_handles: Vec<JoinHandle<Result<(), PollError>>>,
    output_handles: Vec<JoinHandle<Result<(), WriteError>>>,
    transform_handle: JoinHandle<Result<(), TransformError>>,

    /// Controls the pipeline.
    controller: PipelineController,
}

enum PipelineController {
    /// A controller that has not been started yet.
    ///
    /// The control task is lazily started, i.e. it does not exist if no one asks for a [`ControlHandle`].
    Waiting(PipelineControllerState),

    /// Temporary state.
    Temporary,

    /// A controller that has been started.
    ///
    /// The state has been moved to the control task, and we can communicate with it through the [`ControlHandle`].
    Started(ControlHandle),
}

impl Default for PipelineController {
    fn default() -> Self {
        Self::Temporary
    }
}

struct PipelineControllerState {
    // Senders to keep the receivers alive and to send commands.
    source_command_senders_by_plugin: HashMap<String, Vec<watch::Sender<SourceCmd>>>,
    output_command_senders_by_plugin: HashMap<String, Vec<watch::Sender<OutputCmd>>>,

    /// Currently active transforms.
    /// Note: it could be generalized to support more than 64 values,
    /// either with a crate like arc-swap, or by using multiple Vec of transforms, each with an AtomicU64.
    active_transforms: Arc<AtomicU64>,
    transforms_mask_by_plugin: HashMap<String, u64>,
}

#[derive(Clone)]
pub struct ControlHandle {
    tx: mpsc::Sender<ControlMessage>,
}

impl MeasurementPipeline {
    /// Starts the measurement pipeline.
    pub fn start(self) -> RunningPipeline {
        // Store the task handles in order to wait for them to complete before stopping,
        // and the command senders in order to keep the receivers alive and to be able to send commands after the launch.
        let mut source_handles = Vec::with_capacity(self.sources.len() + self.autonomous_sources.len());
        let mut output_handles = Vec::with_capacity(self.outputs.len());
        let mut source_command_senders_by_plugin: HashMap<_, Vec<_>> = HashMap::new();
        let mut output_command_senders_by_plugin: HashMap<_, Vec<_>> = HashMap::new();
        let mut transforms_mask_by_plugin: HashMap<_, u64> = HashMap::new();

        // Start the tasks, starting at the end of the pipeline (to avoid filling the buffers too quickly).
        let (in_tx, in_rx) = self.from_sources;

        // 1. Outputs
        for out in self.outputs {
            let msg_rx = self.to_outputs.subscribe();
            let (command_tx, command_rx) = watch::channel(OutputCmd::Run);
            let ctx = OutputContext {
                // Each output task owns its OutputContext, which contains a copy of the MetricRegistry.
                // This allows fast, uncontended access to the registry, and avoids a global state (no Arc<Mutex<...>>).
                // The cost is a duplication of the registry (increased memory use) in the case where multiple outputs exist.
                metrics: self.metrics.clone(),
            };
            // Spawn the task and store the handle.
            let handle = self
                .rt_normal
                .spawn(run_output_from_broadcast(out.output, msg_rx, command_rx, ctx));
            output_handles.push(handle);
            output_command_senders_by_plugin
                .entry(out.plugin_name)
                .or_default()
                .push(command_tx);
        }

        // 2. Transforms
        let active_transforms = Arc::new(AtomicU64::new(u64::MAX)); // all active by default
        let mut transforms = Vec::with_capacity(self.transforms.len());
        for (i, t) in self.transforms.into_iter().enumerate() {
            transforms.push(t.transform);
            let mask: u64 = 1 << i;
            transforms_mask_by_plugin
                .entry(t.plugin_name)
                .or_default()
                .bitor_assign(mask);
        }
        let transform_handle = self.rt_normal.spawn(run_transforms(
            transforms,
            in_rx,
            self.to_outputs,
            active_transforms.clone(),
        ));

        // 3. Managed sources
        for src in self.sources {
            let data_tx = in_tx.clone();
            let (command_tx, command_rx) = watch::channel(SourceCmd::SetTrigger(Some(src.trigger_provider)));
            let runtime = match src.source_type {
                SourceType::Normal => &self.rt_normal,
                SourceType::RealtimePriority => self.rt_priority.as_ref().unwrap(),
            };
            let handle = runtime.spawn(run_source(src.source, data_tx, command_rx));
            source_handles.push(handle);
            source_command_senders_by_plugin
                .entry(src.plugin_name)
                .or_default()
                .push(command_tx);
        }

        // 4. Autonomous sources
        for src in self.autonomous_sources {
            self.rt_normal.spawn(src);
        }

        RunningPipeline {
            _rt_normal: self.rt_normal,
            _rt_priority: self.rt_priority,
            source_handles,
            output_handles,
            transform_handle,

            // Don't start the control task yet, but be prepared to do it.
            controller: PipelineController::Waiting(PipelineControllerState {
                source_command_senders_by_plugin,
                output_command_senders_by_plugin,
                active_transforms,
                transforms_mask_by_plugin,
            }),
        }
    }
}

#[derive(Clone, Debug)]
pub enum SourceCmd {
    Run,
    Pause,
    Stop,
    SetTrigger(Option<Trigger>),
}

async fn run_source(
    mut source: Box<dyn Source>,
    tx: mpsc::Sender<MeasurementBuffer>,
    mut commands: watch::Receiver<SourceCmd>,
) -> Result<(), PollError> {
    fn init_trigger(provider: &mut Option<Trigger>) -> anyhow::Result<ConfiguredSourceTrigger> {
        provider
            .take()
            .expect("invalid empty trigger in message Init(trigger)")
            .auto_configured()
            .context("init_trigger failed")
    }

    // the first command must be "init"
    let mut trigger = {
        let init_cmd = commands
            .wait_for(|c| matches!(c, SourceCmd::SetTrigger(_)))
            .await
            .expect("watch channel must stay open during run_source");

        match (*init_cmd).clone() {
            // cloning required to borrow opt as mut below
            SourceCmd::SetTrigger(mut opt) => init_trigger(&mut opt)?,
            _ => unreachable!(),
        }
    };

    // Stores measurements in this buffer, and replace it every `flush_rounds` rounds.
    // We probably need the capacity to store at least one measurement per round.
    let mut buffer = MeasurementBuffer::with_capacity(trigger.flush_rounds);

    // main loop
    let mut i = 1usize;
    'run: loop {
        // wait for trigger
        trigger.next().await?;

        // poll the source
        let timestamp = SystemTime::now();
        source.poll(&mut buffer.as_accumulator(), timestamp)?;

        // Flush the measurements and update the command, not on every round for performance reasons.
        // This is done _after_ polling, to ensure that we poll at least once before flushing, even if flush_rounds is 1.
        if i % trigger.flush_rounds == 0 {
            // flush and create a new buffer
            let prev_length = buffer.len(); // hint for the new buffer size, great if the number of measurements per flush doesn't change much
            tx.try_send(buffer)
                .context("todo: handle failed send (source too fast)")?;
            buffer = MeasurementBuffer::with_capacity(prev_length);
            log::debug!("source flushed {prev_length} measurements");

            // update state based on the latest command
            if commands.has_changed().unwrap() {
                let mut paused = false;
                'pause: loop {
                    let cmd = if paused {
                        commands
                            .changed()
                            .await
                            .expect("The output channel of paused source should be open.");
                        (*commands.borrow()).clone()
                    } else {
                        (*commands.borrow_and_update()).clone()
                    };
                    match cmd {
                        SourceCmd::Run => break 'pause,
                        SourceCmd::Pause => paused = true,
                        SourceCmd::Stop => break 'run,
                        SourceCmd::SetTrigger(mut opt) => {
                            trigger = init_trigger(&mut opt)?;
                            let hint_additional_elems = trigger.flush_rounds - (i % trigger.flush_rounds);
                            buffer.reserve(hint_additional_elems);
                            if !paused {
                                break 'pause;
                            }
                        }
                    }
                }
            }
        }
        i = i.wrapping_add(1);
    }
    Ok(())
}

#[derive(Debug)]
pub enum TransformCmd {
    Enable,
    Disable,
}

async fn run_transforms(
    mut transforms: Vec<Box<dyn Transform>>,
    mut rx: mpsc::Receiver<MeasurementBuffer>,
    tx: broadcast::Sender<OutputMsg>,
    active_flags: Arc<AtomicU64>,
) -> Result<(), TransformError> {
    loop {
        if let Some(mut measurements) = rx.recv().await {
            // Update the list of active transforms (the PipelineController can update the flags).
            let current_flags = active_flags.load(Ordering::Relaxed);

            // Run the enabled transforms. If one of them fails, we cannot continue.
            for (i, t) in &mut transforms.iter_mut().enumerate() {
                let t_flag = 1 << i;
                if current_flags & t_flag != 0 {
                    t.apply(&mut measurements)?;
                }
            }

            // Send the results to the outputs.
            tx.send(OutputMsg::WriteMeasurements(measurements))
                .context("could not send the measurements from transforms to the outputs")?;
        } else {
            log::warn!("The channel connected to the transform step has been closed, the transforms will stop.");
            break;
        }
    }
    Ok(())
}

/// A command for an output.
#[derive(Clone, PartialEq, Eq)]
pub enum OutputCmd {
    Run,
    Pause,
    Stop,
}

#[derive(Debug, Clone)]
pub enum OutputMsg {
    WriteMeasurements(MeasurementBuffer),
    RegisterMetrics {
        metrics: Vec<Metric>,
        source_name: String,
        reply_to: tokio::sync::mpsc::Sender<Vec<RawMetricId>>,
    },
}

async fn run_output_from_broadcast(
    mut output: Box<dyn Output>,
    mut rx: broadcast::Receiver<OutputMsg>,
    mut commands: watch::Receiver<OutputCmd>,
    mut ctx: OutputContext,
) -> Result<(), WriteError> {
    // Two possible designs:
    // A) Use one mpsc channel + one shared variable that contains the current command,
    // - when a message is received, check the command and act accordingly
    // - to change the command, update the variable and send a special message through the channel
    // In this alternative design, each Output would have one mpsc channel, and the Transform step would call send() or try_send() on each of them.
    //
    // B) use a broadcast + watch, where the broadcast discards old values when a receiver (output) lags behind, instead of either (with option A):
    // - preventing the transform from running (mpsc channel's send() blocks when the queue is full).
    // - losing the most recent messages in transform, for one output. Other outputs that are not lagging behind will receive all messages fine, since try_send() does not block.
    //     The problem is: what to do with messages that could not be sent, when try_send() fails?

    async fn handle_message(
        received_msg: OutputMsg,
        output: &mut dyn Output,
        ctx: &mut OutputContext,
    ) -> Result<(), WriteError> {
        match received_msg {
            OutputMsg::WriteMeasurements(measurements) => {
                // output.write() is blocking, do it in a dedicated thread.

                // Output is not Sync, we could move the value to the future and back (idem for ctx),
                // but that would likely introduce a needless copy, and would be cumbersome to work with.
                // Instead, we use the `scoped` module.
                let res =
                    scoped::spawn_blocking_with_output(output, ctx, move |out, ctx| out.write(&measurements, &ctx))
                        .await;
                match res {
                    Ok(write_res) => {
                        if let Err(e) = write_res {
                            log::error!("Output failed: {:?}", e); // todo give a name to the output
                        }
                        Ok(())
                    }
                    Err(await_err) => {
                        if await_err.is_panic() {
                            return Err(anyhow!(
                                "A blocking writing task panicked, there is a bug somewhere! Details: {}",
                                await_err
                            )
                            .into());
                        } else {
                            todo!("unhandled error");
                        }
                    }
                }
            }
            OutputMsg::RegisterMetrics {
                metrics,
                source_name,
                reply_to,
            } => {
                let metric_ids = ctx.metrics.extend_infallible(metrics, &source_name);
                reply_to.send(metric_ids).await?;
                Ok(())
            }
        }
    }

    loop {
        tokio::select! {
            received_cmd = commands.changed() => {
                // Process new command, clone it to quickly end the borrow (which releases the internal lock as suggested by the doc)
                match received_cmd.map(|_| commands.borrow().clone()) {
                    Ok(OutputCmd::Run) => (), // continue running
                    Ok(OutputCmd::Pause) => {
                        // wait for the command to change
                        match commands.wait_for(|cmd| cmd != &OutputCmd::Pause).await {
                            Ok(new_cmd) => match *new_cmd {
                                OutputCmd::Run => (), // exit the wait
                                OutputCmd::Stop => break, // stop the loop
                                OutputCmd::Pause => unreachable!(),
                            },
                            Err(_) => todo!("watch channel closed"),
                        };
                    },
                    Ok(OutputCmd::Stop) => break, // stop the loop
                    Err(_) => todo!("watch channel closed")
                }
            },
            received_msg = rx.recv() => {
                match received_msg {
                    Ok(msg) => {
                        handle_message(msg, output.as_mut(), &mut ctx).await?;
                    },
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!("Output is too slow, it lost the oldest {n} messages.");
                    },
                    Err(broadcast::error::RecvError::Closed) => {
                        log::warn!("The channel connected to output was closed, it will now stop.");
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

impl RunningPipeline {
    /// Blocks the current thread until all tasks in the pipeline finish.
    pub fn wait_for_all(&mut self) {
        self._rt_normal.block_on(async {
            for handle in &mut self.source_handles {
                handle.await.unwrap().unwrap(); // todo: handle errors
            }

            (&mut self.transform_handle).await.unwrap().unwrap();

            for handle in &mut self.output_handles {
                handle.await.unwrap().unwrap();
            }
        });
    }

    /// Returns a [`ControlHandle`], which allows to change the configuration
    /// of the pipeline while it is running.
    pub fn control_handle(&mut self) -> ControlHandle {
        fn handle_message(state: &mut PipelineControllerState, msg: ControlMessage) {
            match msg {
                ControlMessage::Source(plugin_name, cmd) => {
                    if let Some(plugin) = plugin_name {
                        for s in state.source_command_senders_by_plugin.get(&plugin).unwrap() {
                            s.send(cmd.clone()).unwrap();
                        }
                    } else {
                        for senders in state.source_command_senders_by_plugin.values() {
                            for s in senders {
                                s.send(cmd.clone()).unwrap();
                            }
                        }
                    }
                }
                ControlMessage::Output(plugin_name, cmd) => {
                    if let Some(plugin) = plugin_name {
                        for s in state.output_command_senders_by_plugin.get(&plugin).unwrap() {
                            s.send(cmd.clone()).unwrap();
                        }
                    } else {
                        for senders in state.output_command_senders_by_plugin.values() {
                            for s in senders {
                                s.send(cmd.clone()).unwrap();
                            }
                        }
                    }
                }
                ControlMessage::Transform(plugin_name, cmd) => {
                    let mask: u64 = if let Some(plugin) = plugin_name {
                        *state.transforms_mask_by_plugin.get(&plugin).unwrap()
                    } else {
                        u64::MAX
                    };
                    match cmd {
                        TransformCmd::Enable => state.active_transforms.fetch_or(mask, Ordering::Relaxed),
                        TransformCmd::Disable => state.active_transforms.fetch_nand(mask, Ordering::Relaxed),
                    };
                }
            }
        }

        let handle = match std::mem::take(&mut self.controller) {
            PipelineController::Waiting(mut state) => {
                // This is the first handle, start the control task and move the state to it.
                let (tx, mut rx) = mpsc::channel::<ControlMessage>(256);
                self._rt_normal.spawn(async move {
                    loop {
                        if let Some(msg) = rx.recv().await {
                            handle_message(&mut state, msg);
                        } else {
                            break; // channel closed
                        }
                    }
                });
                ControlHandle { tx }
            }
            PipelineController::Temporary => unreachable!(),
            PipelineController::Started(handle) => {
                // This is NOT the first handle, return a clone of the existing handle.
                handle
            }
        };
        let cloned = handle.clone();
        self.controller = PipelineController::Started(handle);
        cloned
    }
}

impl ControlHandle {
    pub fn all(&self) -> ScopedControlHandle {
        ScopedControlHandle {
            handle: self,
            plugin_name: None,
        }
    }
    pub fn plugin(&self, plugin_name: impl Into<String>) -> ScopedControlHandle {
        ScopedControlHandle {
            handle: self,
            plugin_name: Some(plugin_name.into()),
        }
    }

    pub fn blocking_all(&self) -> BlockingControlHandle {
        BlockingControlHandle {
            handle: self,
            plugin_name: None,
        }
    }

    pub fn blocking_plugin(&self, plugin_name: impl Into<String>) -> BlockingControlHandle {
        BlockingControlHandle {
            handle: self,
            plugin_name: Some(plugin_name.into()),
        }
    }
}

pub struct ScopedControlHandle<'a> {
    handle: &'a ControlHandle,
    plugin_name: Option<String>,
}
impl<'a> ScopedControlHandle<'a> {
    pub async fn control_sources(self, cmd: SourceCmd) {
        self.handle
            .tx
            .send(ControlMessage::Source(self.plugin_name, cmd))
            .await
            .unwrap();
    }
    pub async fn control_transforms(self, cmd: TransformCmd) {
        self.handle
            .tx
            .send(ControlMessage::Transform(self.plugin_name, cmd))
            .await
            .unwrap();
    }
    pub async fn control_outputs(self, cmd: OutputCmd) {
        self.handle
            .tx
            .send(ControlMessage::Output(self.plugin_name, cmd))
            .await
            .unwrap();
    }
}

pub struct BlockingControlHandle<'a> {
    handle: &'a ControlHandle,
    plugin_name: Option<String>,
}
impl<'a> BlockingControlHandle<'a> {
    pub fn control_sources(self, cmd: SourceCmd) {
        self.handle
            .tx
            .blocking_send(ControlMessage::Source(self.plugin_name, cmd))
            .unwrap();
    }
    pub fn control_transforms(self, cmd: TransformCmd) {
        self.handle
            .tx
            .blocking_send(ControlMessage::Transform(self.plugin_name, cmd))
            .unwrap();
    }
    pub fn control_outputs(self, cmd: OutputCmd) {
        self.handle
            .tx
            .blocking_send(ControlMessage::Output(self.plugin_name, cmd))
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering},
            Arc,
        },
        thread::sleep,
        time::{Duration, Instant},
    };

    use tokio::{
        runtime::Runtime,
        sync::{broadcast, mpsc, watch},
    };

    use crate::{
        measurement::{MeasurementBuffer, MeasurementPoint, WrappedMeasurementType, WrappedMeasurementValue},
        metrics::{MetricRegistry, RawMetricId},
        pipeline::{trigger::Trigger, OutputContext, Transform},
        resources::ResourceId,
    };

    use super::{run_output_from_broadcast, run_source, run_transforms, OutputCmd, OutputMsg, SourceCmd};

    #[test]
    fn source_triggered_by_time() {
        let rt = new_rt(2);

        let source = TestSource::new();
        let tp = new_tp();
        let (tx, mut rx) = mpsc::channel::<MeasurementBuffer>(64);
        let (cmd_tx, cmd_rx) = watch::channel(SourceCmd::SetTrigger(Some(tp)));

        let stopped = Arc::new(AtomicU8::new(TestSourceState::Running as _));
        let stopped2 = stopped.clone();
        rt.spawn(async move {
            let mut n_polls = 0;
            loop {
                // check the measurements
                if let Some(measurements) = rx.recv().await {
                    assert_ne!(
                        TestSourceState::Stopped as u8,
                        stopped2.load(Ordering::Relaxed),
                        "The source is stopped/paused, it should not produce measurements."
                    );

                    // 2 by 2 because flush_interval = 2*poll_interval
                    assert_eq!(measurements.len(), 2);
                    n_polls += 2;
                    let last_point = measurements.iter().last().unwrap();
                    let last_point_value = match last_point.value {
                        WrappedMeasurementValue::U64(n) => n,
                        _ => panic!("unexpected value type"),
                    };
                    assert_eq!(n_polls, last_point_value);
                } else {
                    // the channel is dropped when run_source terminates, which must only occur when the source is stopped
                    assert_ne!(
                        TestSourceState::Running as u8,
                        stopped2.load(Ordering::Relaxed),
                        "The source is not stopped, the channel should be open."
                    );
                }
            }
        });

        // poll the source for some time
        rt.spawn(run_source(Box::new(source), tx, cmd_rx));
        sleep(Duration::from_millis(20));

        // pause source
        cmd_tx.send(SourceCmd::Pause).unwrap();
        stopped.store(TestSourceState::Stopping as _, Ordering::Relaxed);
        sleep(Duration::from_millis(10)); // some tolerance (wait for flushing)
        stopped.store(TestSourceState::Stopped as _, Ordering::Relaxed);

        // check that the source is paused
        sleep(Duration::from_millis(10));

        // still paused after SetTrigger
        cmd_tx.send(SourceCmd::SetTrigger(Some(new_tp()))).unwrap();
        sleep(Duration::from_millis(20));

        // resume source
        cmd_tx.send(SourceCmd::Run).unwrap();
        stopped.store(TestSourceState::Running as _, Ordering::Relaxed);
        sleep(Duration::from_millis(5)); // lower tolerance (no flushing, just waiting for changes on the watch channel)

        // poll for some time
        sleep(Duration::from_millis(10));

        // still running after SetTrigger
        cmd_tx.send(SourceCmd::SetTrigger(Some(new_tp()))).unwrap();
        sleep(Duration::from_millis(20));

        // stop source
        cmd_tx.send(SourceCmd::Stop).unwrap();
        stopped.store(TestSourceState::Stopping as _, Ordering::Relaxed);
        sleep(Duration::from_millis(10)); // some tolerance
        stopped.store(TestSourceState::Stopped as _, Ordering::Relaxed);

        // check that the source is stopped
        sleep(Duration::from_millis(20));

        // drop the runtime, abort the tasks
    }

    #[test]
    fn transform_task() {
        let rt = new_rt(2);

        // create transforms
        let check_input_type_for_transform3 = Arc::new(AtomicBool::new(true));
        let transforms: Vec<Box<dyn Transform>> = vec![
            Box::new(TestTransform {
                id: 1,
                expected_input_len: 2, // 2 because flush_interval = 2*poll_interval
                output_type: WrappedMeasurementType::U64,
                expected_input_type: WrappedMeasurementType::U64,
                check_input_type: Arc::new(AtomicBool::new(true)),
            }),
            Box::new(TestTransform {
                id: 2,
                expected_input_len: 2,
                output_type: WrappedMeasurementType::F64,
                expected_input_type: WrappedMeasurementType::U64,
                check_input_type: Arc::new(AtomicBool::new(true)),
            }),
            Box::new(TestTransform {
                id: 3,
                expected_input_len: 2,
                output_type: WrappedMeasurementType::F64,
                expected_input_type: WrappedMeasurementType::F64,
                check_input_type: check_input_type_for_transform3.clone(),
            }),
        ];

        // create source
        let source = TestSource::new();
        let tp = new_tp();
        let (src_tx, src_rx) = mpsc::channel::<MeasurementBuffer>(64);
        let (_src_cmd_tx, src_cmd_rx) = watch::channel(SourceCmd::SetTrigger(Some(tp)));

        // create transform channels and control flags
        let (trans_tx, mut out_rx) = broadcast::channel::<OutputMsg>(64);
        let active_flags = Arc::new(AtomicU64::new(u64::MAX));
        let active_flags2 = active_flags.clone();
        let active_flags3 = active_flags.clone();

        rt.spawn(async move {
            loop {
                if let Ok(OutputMsg::WriteMeasurements(measurements)) = out_rx.recv().await {
                    let current_flags = active_flags2.load(Ordering::Relaxed);
                    let transform1_enabled = current_flags & 1 != 0;
                    let transform2_enabled = current_flags & 2 != 0;
                    let transform3_enabled = current_flags & 4 != 0;
                    for m in measurements.iter() {
                        let int_val = match m.value {
                            WrappedMeasurementValue::F64(f) => f as u32,
                            WrappedMeasurementValue::U64(u) => u as u32,
                        };
                        if transform3_enabled {
                            assert_eq!(int_val, 3);
                            assert_eq!(m.value.measurement_type(), WrappedMeasurementType::F64);
                        } else if transform2_enabled {
                            assert_eq!(int_val, 2);
                            assert_eq!(m.value.measurement_type(), WrappedMeasurementType::F64);
                        } else if transform1_enabled {
                            assert_eq!(int_val, 1);
                            assert_eq!(m.value.measurement_type(), WrappedMeasurementType::U64);
                        } else {
                            assert_ne!(int_val, 3);
                            assert_ne!(int_val, 2);
                            assert_ne!(int_val, 1);
                            assert_eq!(m.value.measurement_type(), WrappedMeasurementType::U64);
                        }
                    }
                }
            }
        });

        // run the transforms
        rt.spawn(run_transforms(transforms, src_rx, trans_tx, active_flags3));

        // poll the source for some time
        rt.spawn(run_source(Box::new(source), src_tx, src_cmd_rx));
        sleep(Duration::from_millis(20));

        // disable transform 3 only
        active_flags.store(1 | 2, Ordering::Relaxed);
        sleep(Duration::from_millis(20));

        // disable transform 1 only
        active_flags.store(2 | 4, Ordering::Relaxed);
        sleep(Duration::from_millis(20));

        // disable transform 2 only, the input type expected by transform 3 is no longer respected
        check_input_type_for_transform3.store(false, Ordering::Relaxed);
        active_flags.store(1 | 4, Ordering::Relaxed);
        sleep(Duration::from_millis(20));

        // disable all transforms
        active_flags.store(0, Ordering::Relaxed);
        check_input_type_for_transform3.store(true, Ordering::Relaxed);
        sleep(Duration::from_millis(20));

        // enable all transforms
        active_flags.store(1 | 2 | 4, Ordering::Relaxed);
        sleep(Duration::from_millis(20));
    }

    #[test]
    fn output_task() {
        let rt = new_rt(3);
        // create source
        let source = Box::new(TestSource::new());
        let tp = new_tp();
        let (src_tx, trans_rx) = mpsc::channel::<MeasurementBuffer>(64);
        let (src_cmd_tx, src_cmd_rx) = watch::channel(SourceCmd::SetTrigger(Some(tp)));

        // no transforms but a transform task to send the values to the output
        let transforms = vec![];
        let (trans_tx, out_rx) = broadcast::channel::<OutputMsg>(64);
        let active_flags = Arc::new(AtomicU64::new(u64::MAX));

        // create output
        let output_count = Arc::new(AtomicU32::new(0));
        let output = Box::new(TestOutput {
            expected_input_len: 2,
            output_count: output_count.clone(),
        });
        let (out_cmd_tx, out_cmd_rx) = watch::channel(OutputCmd::Run);
        let out_ctx = OutputContext {
            metrics: MetricRegistry::new(),
        };

        // start tasks
        rt.spawn(run_output_from_broadcast(output, out_rx, out_cmd_rx, out_ctx));
        rt.spawn(run_transforms(transforms, trans_rx, trans_tx, active_flags));
        rt.spawn(run_source(source, src_tx, src_cmd_rx));

        // check the output
        sleep(Duration::from_millis(20));
        assert!(output_count.load(Ordering::Relaxed).abs_diff(4) <= 2);

        // pause and check
        out_cmd_tx.send(OutputCmd::Pause).unwrap();
        let count_at_pause = output_count.load(Ordering::Relaxed);
        sleep(Duration::from_millis(10));
        assert!(output_count.load(Ordering::Relaxed).abs_diff(count_at_pause) <= 2);
        sleep(Duration::from_millis(20));

        // resume and check
        let count_before_resume = output_count.load(Ordering::Relaxed);
        out_cmd_tx.send(OutputCmd::Run).unwrap();
        sleep(Duration::from_millis(20));
        assert!(output_count.load(Ordering::Relaxed) > count_before_resume);

        // stop and check
        src_cmd_tx.send(SourceCmd::Stop).unwrap();
        out_cmd_tx.send(OutputCmd::Stop).unwrap();
        sleep(Duration::from_millis(10));
        let count = output_count.load(Ordering::Relaxed);
        sleep(Duration::from_millis(20));
        assert_eq!(count, output_count.load(Ordering::Relaxed));
    }

    fn new_tp() -> Trigger {
        Trigger::TimeInterval {
            start_time: Instant::now(),
            poll_interval: Duration::from_millis(5),
            flush_interval: Duration::from_millis(10),
        }
    }

    fn new_rt(n_threads: usize) -> Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(n_threads)
            .enable_all()
            .build()
            .unwrap()
    }

    enum TestSourceState {
        /// The source must be running.
        Running = 0,
        /// The source has been stopped but can continue to run for some time
        /// before the next refresh of the commands (which occurs at the same time as flushing).
        Stopping = 1,
        /// The source has been stopped and should have been refreshed, no measurements must be produced.
        Stopped = 2,
    }

    struct TestSource {
        n_calls: u32,
    }
    impl TestSource {
        fn new() -> TestSource {
            TestSource { n_calls: 0 }
        }
    }
    impl crate::pipeline::Source for TestSource {
        fn poll(
            &mut self,
            into: &mut crate::measurement::MeasurementAccumulator,
            time: std::time::SystemTime,
        ) -> Result<(), crate::pipeline::PollError> {
            self.n_calls += 1;
            let point = MeasurementPoint::new_untyped(
                time,
                RawMetricId(1),
                ResourceId::LocalMachine,
                WrappedMeasurementValue::U64(self.n_calls as u64),
            );
            into.push(point);
            Ok(())
        }
    }

    struct TestTransform {
        id: u32,
        output_type: WrappedMeasurementType,
        expected_input_len: usize,
        expected_input_type: WrappedMeasurementType,
        check_input_type: Arc<AtomicBool>,
    }

    impl crate::pipeline::Transform for TestTransform {
        fn apply(&mut self, measurements: &mut MeasurementBuffer) -> Result<(), crate::pipeline::TransformError> {
            assert_eq!(measurements.len(), self.expected_input_len);
            for m in measurements.iter_mut() {
                assert_eq!(m.resource, ResourceId::LocalMachine);
                if self.check_input_type.load(Ordering::Relaxed) {
                    assert_eq!(m.value.measurement_type(), self.expected_input_type);
                }
                m.value = match self.output_type {
                    WrappedMeasurementType::F64 => WrappedMeasurementValue::F64(self.id as _),
                    WrappedMeasurementType::U64 => WrappedMeasurementValue::U64(self.id as _),
                };
            }
            assert_eq!(measurements.len(), self.expected_input_len);
            Ok(())
        }
    }

    struct TestOutput {
        expected_input_len: usize,
        output_count: Arc<AtomicU32>,
    }

    impl crate::pipeline::Output for TestOutput {
        fn write(
            &mut self,
            measurements: &MeasurementBuffer,
            _ctx: &OutputContext,
        ) -> Result<(), crate::pipeline::WriteError> {
            assert_eq!(measurements.len(), self.expected_input_len);
            self.output_count.fetch_add(measurements.len() as _, Ordering::Relaxed);
            Ok(())
        }
    }
}
