use prometheus::{
    HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};

#[derive(Clone)]
pub struct Metrics {
    registry: Registry,
    pub admitted: IntGauge,
    pub rejections: IntCounterVec,
    pub queue_depth: IntGauge,
    pub outcomes: IntCounterVec,
    pub stage: HistogramVec,
}

impl Metrics {
    pub fn new() -> anyhow::Result<Self> {
        let registry = Registry::new();
        let admitted = IntGauge::new(
            "fleetcore_admitted_work",
            "Work holding a shared admission permit",
        )?;
        let rejections = IntCounterVec::new(
            Opts::new(
                "fleetcore_admission_rejections_total",
                "Admission rejections",
            ),
            &["transport", "reason"],
        )?;
        registry.register(Box::new(admitted.clone()))?;
        registry.register(Box::new(rejections.clone()))?;
        let queue_depth = IntGauge::new("fleetcore_queue_depth", "Batches awaiting a worker")?;
        let outcomes = IntCounterVec::new(
            Opts::new(
                "fleetcore_ingestion_total",
                "Ingestion outcomes by transport",
            ),
            &["transport", "outcome"],
        )?;
        let stage = HistogramVec::new(
            HistogramOpts::new("fleetcore_stage_seconds", "Ingestion stage duration").buckets(
                vec![
                    0.0001, 0.0005, 0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0,
                    2.5, 5.0, 10.0, 30.0,
                ],
            ),
            &["transport", "stage"],
        )?;
        registry.register(Box::new(queue_depth.clone()))?;
        registry.register(Box::new(outcomes.clone()))?;
        registry.register(Box::new(stage.clone()))?;
        Ok(Self {
            registry,
            admitted,
            rejections,
            queue_depth,
            outcomes,
            stage,
        })
    }

    pub fn render(&self) -> Result<String, prometheus::Error> {
        let mut output = String::new();
        TextEncoder::new().encode_utf8(&self.registry.gather(), &mut output)?;
        Ok(output)
    }
}
