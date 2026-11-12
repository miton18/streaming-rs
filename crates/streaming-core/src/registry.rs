//! The operator registry: the bridge between the references stored in a graph
//! description (`acme/decode-event@1`) and the Rust code compiled into a worker.

use crate::{
    builder::Operator,
    clock::{Clock, SecretProvider},
    data::{Datum, Key, datum},
    ids::{GraphId, NodeId, OpRef, TenantId},
    kv::DynKv,
    ops::{
        Aggregator, DynAggregator, DynKey, DynMap, OpError, SinkConnector, SourceConnector, TypedAggregator,
        WindowAssigner, downcast,
    },
    param::Secret,
    pipe::{self, Built, Sink, Source, Transformer},
};
use moonpool_core::{Providers, TimeProvider};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, HashMap},
    fmt,
    sync::Arc,
    time::Duration,
};

/// What a node does in a graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OpKind {
    /// Reads from the outside world.
    Source,
    /// Stateless transformation (`map`, `filter`, `flat_map`).
    Map,
    /// Annotates records with a grouping key.
    KeyBy,
    /// Annotates records with their event-time window(s).
    Window,
    /// Stateful aggregation per key and window.
    Aggregate,
    /// Writes to the outside world.
    Sink,
}

impl fmt::Display for OpKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            OpKind::Source => "source",
            OpKind::Map => "map",
            OpKind::KeyBy => "key_by",
            OpKind::Window => "window",
            OpKind::Aggregate => "aggregate",
            OpKind::Sink => "sink",
        })
    }
}

/// The type of the values an operator accepts or produces.
///
/// Types are compared by name, which is enough to reject a mis-wired graph at
/// submission time. `Any` matches everything; it is what type-agnostic
/// operators (windows, `std/count@1`, ...) declare.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TypeTag {
    /// Matches any type.
    Any,
    /// A concrete Rust type, by name.
    Named(String),
}

impl TypeTag {
    /// The tag of a Rust type.
    #[must_use]
    pub fn of<T: ?Sized + 'static>() -> Self { TypeTag::Named(std::any::type_name::<T>().to_owned()) }

    /// Whether a value of type `self` may flow into an input declared `other`.
    #[must_use]
    pub fn flows_into(&self, other: &TypeTag) -> bool {
        match (self, other) {
            (TypeTag::Any, _) | (_, TypeTag::Any) => true,
            (TypeTag::Named(a), TypeTag::Named(b)) => a == b,
        }
    }
}

impl fmt::Display for TypeTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypeTag::Any => f.write_str("*"),
            TypeTag::Named(name) => f.write_str(name),
        }
    }
}

impl Serialize for TypeTag {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> { serializer.collect_str(self) }
}

impl schemars::JsonSchema for TypeTag {
    fn schema_name() -> std::borrow::Cow<'static, str> { "TypeTag".into() }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "description": "A value type: `*` for any type, otherwise a Rust type name.",
        })
    }
}

impl<'de> Deserialize<'de> for TypeTag {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(if s == "*" { TypeTag::Any } else { TypeTag::Named(s) })
    }
}

/// Public description of a registered operator. Published in the KV so that
/// the API and the UI can list operators and validate graphs without the code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OpDescriptor {
    /// The reference used in graph descriptions.
    pub op: OpRef,
    /// What the operator does.
    pub kind: OpKind,
    /// Type of the accepted values (`*` for sources and type-agnostic operators).
    pub input: TypeTag,
    /// Type of the produced values (`*` for sinks and pass-through operators).
    pub output: TypeTag,
    /// One-line human description.
    #[serde(default)]
    pub description: String,
    /// The parameters of its configuration the API and the UI may change.
    /// Empty when the operator exposes none.
    #[serde(default)]
    pub params: Vec<ParamSpec>,
    /// What the operator takes and gives, case by case, when it is written
    /// for several types and its output depends on its input. Empty
    /// otherwise: `input` and `output` say it all.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub signatures: Vec<Signature>,
}

/// One of the types an operator accepts, and what it produces from it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Signature {
    /// Type of the accepted values.
    pub input: TypeTag,
    /// Type of the values produced from them.
    pub output: TypeTag,
}

impl OpDescriptor {
    /// What the operator produces from values of type `produced`, when it
    /// accepts them. [`TypeTag::Any`] stands for "the type it was given".
    #[must_use]
    pub fn output_for(&self, produced: &TypeTag) -> Option<TypeTag> {
        if self.signatures.is_empty() {
            return produced.flows_into(&self.input).then(|| self.output.clone());
        }
        self.signatures
            .iter()
            .find(|signature| signature.input == *produced)
            .or_else(|| {
                self.signatures
                    .iter()
                    .find(|signature| produced.flows_into(&signature.input))
            })
            .map(|signature| signature.output.clone())
    }
}

/// One parameter of an operator's configuration, as a tool presents it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ParamSpec {
    /// Key in the configuration object.
    pub name: String,
    /// `duration` (`"5m"`), `integer`, `number`, `text`, `boolean`,
    /// `secret` (a text or a `{"secret": "<name>"}` reference), or `json`
    /// (any JSON value, such as an object).
    pub kind: String,
    /// Whether the operator refuses a configuration without it.
    #[serde(default)]
    pub required: bool,
    /// Value used when absent.
    #[serde(default)]
    pub default: Option<serde_json::Value>,
    /// What it does.
    #[serde(default)]
    pub description: String,
    /// The only values accepted, when the parameter is one of a fixed set of
    /// texts. Empty when any value of `kind` is.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub choices: Vec<String>,
}

impl ParamSpec {
    /// An optional parameter.
    #[must_use]
    pub fn new(name: &str, kind: &str, description: &str) -> Self {
        Self {
            name: name.to_owned(),
            kind: kind.to_owned(),
            required: false,
            default: None,
            description: description.to_owned(),
            choices: Vec::new(),
        }
    }

    /// Make it mandatory.
    #[must_use]
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    /// Give the value used when absent.
    #[must_use]
    pub fn default(mut self, value: impl Into<serde_json::Value>) -> Self {
        self.default = Some(value.into());
        self
    }

    /// Restrict it to a fixed set of texts.
    #[must_use]
    pub fn choices<'a>(mut self, choices: impl IntoIterator<Item = &'a str>) -> Self {
        self.choices = choices.into_iter().map(str::to_owned).collect();
        self
    }
}

/// The set of operators a cluster can run, without their code.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Catalog {
    /// Descriptors, by reference.
    pub ops: BTreeMap<OpRef, OpDescriptor>,
}

impl Catalog {
    /// Look an operator up.
    #[must_use]
    pub fn get(&self, op: &OpRef) -> Option<&OpDescriptor> { self.ops.get(op) }

    /// Add or replace a descriptor.
    pub fn insert(&mut self, descriptor: OpDescriptor) { self.ops.insert(descriptor.op.clone(), descriptor); }
}

/// Values the operators of a process share, one per type: a pool of
/// connections, the topics of an in-memory broker.
#[derive(Default)]
pub struct Shared {
    values: Mutex<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl Shared {
    /// The value of type `T`, created on first use.
    #[must_use]
    pub fn get<T: Default + Send + Sync + 'static>(&self) -> Arc<T> {
        let mut values = self.values.lock();
        let value = values
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Arc::new(T::default()) as Arc<dyn Any + Send + Sync>)
            .clone();
        drop(values);
        value.downcast::<T>().unwrap_or_else(|_| Arc::new(T::default()))
    }
}

impl Shared {
    /// The value of type `T`, if one was given or created.
    #[must_use]
    pub fn find<T: Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        let value = self.values.lock().get(&TypeId::of::<T>()).cloned()?;
        value.downcast::<T>().ok()
    }

    /// Give the value of type `T`, in place of the one created on first use.
    pub fn insert<T: Send + Sync + 'static>(&self, value: Arc<T>) {
        self.values
            .lock()
            .insert(TypeId::of::<T>(), value as Arc<dyn Any + Send + Sync>);
    }
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("values", &self.values.lock().len())
            .finish()
    }
}

/// What an operator receives when a node is instantiated.
pub struct OpContext<P: Providers> {
    /// Runtime providers (network, time, tasks, randomness, storage).
    pub providers: P,
    /// Wall clock.
    pub clock: Arc<dyn Clock>,
    /// Secret resolution, to be called with [`OpContext::tenant`].
    pub secrets: Arc<dyn SecretProvider>,
    /// The store of the worker, for operators that keep what they are given
    /// where the API reads it.
    pub kv: DynKv,
    /// What the operators of this process share.
    pub shared: Arc<Shared>,
    /// Tenant owning the graph.
    pub tenant: TenantId,
    /// Graph being instantiated.
    pub graph: GraphId,
    /// Node being instantiated.
    pub node: NodeId,
}

impl<P: Providers> Clone for OpContext<P> {
    fn clone(&self) -> Self {
        Self {
            providers: self.providers.clone(),
            clock: self.clock.clone(),
            secrets: self.secrets.clone(),
            kv: self.kv.clone(),
            shared: self.shared.clone(),
            tenant: self.tenant.clone(),
            graph: self.graph.clone(),
            node: self.node.clone(),
        }
    }
}

impl<P: Providers> OpContext<P> {
    /// The value of type `T` the operators of this process share, created on
    /// first use.
    #[must_use]
    pub fn shared<T: Default + Send + Sync + 'static>(&self) -> Arc<T> { self.shared.get::<T>() }

    /// Wait, on the clock of the runtime (simulated in a simulation).
    pub async fn sleep(&self, duration: Duration) { let _ = self.providers.time().sleep(duration).await; }

    /// The text of a secret of the configuration.
    ///
    /// # Errors
    ///
    /// The tenant has no secret of that name.
    pub fn reveal(&self, secret: &Secret) -> Result<String, OpError> {
        match secret {
            Secret::Literal(text) => Ok(text.clone()),
            Secret::Named(name) => self
                .secrets
                .get(&self.tenant, name)
                .ok_or_else(|| OpError::Config(format!("secret `{name}` is not defined for tenant `{}`", self.tenant))),
        }
    }

    /// Resolve a configuration value that is either a literal string or a
    /// `{"secret": "<name>"}` reference to one of the tenant's secrets.
    pub fn secret_or_literal(&self, value: &Value) -> Result<String, OpError> {
        match value {
            Value::String(literal) => Ok(literal.clone()),
            Value::Object(map) => {
                let name = map
                    .get("secret")
                    .and_then(Value::as_str)
                    .ok_or_else(|| OpError::Config("expected `{\"secret\": \"<name>\"}`".into()))?;
                self.secrets.get(&self.tenant, name).ok_or_else(|| {
                    OpError::Config(format!("secret `{name}` is not defined for tenant `{}`", self.tenant))
                })
            }
            _ => Err(OpError::Config(
                "expected a string or a `{\"secret\": ...}` reference".into(),
            )),
        }
    }
}

type SourceFactory<P> = dyn Fn(&OpContext<P>, &Value) -> Result<Arc<dyn SourceConnector>, OpError> + Send + Sync;
type SinkFactory<P> = dyn Fn(&OpContext<P>, &Value) -> Result<Arc<dyn SinkConnector>, OpError> + Send + Sync;
type MapFactory = dyn Fn(&Value) -> Result<Arc<dyn DynMap>, OpError> + Send + Sync;
type KeyFactory = dyn Fn(&Value) -> Result<Arc<dyn DynKey>, OpError> + Send + Sync;
type WindowFactory = dyn Fn(&Value) -> Result<Arc<dyn WindowAssigner>, OpError> + Send + Sync;
type AggregateFactory = dyn Fn(&Value) -> Result<Arc<dyn DynAggregator>, OpError> + Send + Sync;

/// How to instantiate an operator from a node configuration.
pub enum OpFactory<P: Providers> {
    /// Builds a source connector.
    Source(Arc<SourceFactory<P>>),
    /// Builds a sink connector.
    Sink(Arc<SinkFactory<P>>),
    /// Builds a stateless transformation.
    Map(Arc<MapFactory>),
    /// Builds a key extractor.
    Key(Arc<KeyFactory>),
    /// Builds a window assigner.
    Window(Arc<WindowFactory>),
    /// Builds an aggregator.
    Aggregate(Arc<AggregateFactory>),
}

impl<P: Providers> Clone for OpFactory<P> {
    fn clone(&self) -> Self {
        match self {
            OpFactory::Source(f) => OpFactory::Source(f.clone()),
            OpFactory::Sink(f) => OpFactory::Sink(f.clone()),
            OpFactory::Map(f) => OpFactory::Map(f.clone()),
            OpFactory::Key(f) => OpFactory::Key(f.clone()),
            OpFactory::Window(f) => OpFactory::Window(f.clone()),
            OpFactory::Aggregate(f) => OpFactory::Aggregate(f.clone()),
        }
    }
}

/// One way of building an operator: for the type it takes.
struct Variant<P: Providers> {
    signature: Signature,
    factory: OpFactory<P>,
}

/// A registered operator. It has one variant, or one per type it was
/// registered for when it is written for several.
struct Entry<P: Providers> {
    descriptor: OpDescriptor,
    variants: Vec<Variant<P>>,
    /// Whether a configuration can run, for an operator whose factory needs
    /// the context of a node to say so: a source or a sink.
    accepts: Option<Accepts>,
}

/// Refuses a configuration that cannot run.
type Accepts = Arc<dyn Fn(&Value) -> Result<(), OpError> + Send + Sync>;

impl<P: Providers> Clone for Entry<P> {
    fn clone(&self) -> Self {
        Self {
            descriptor: self.descriptor.clone(),
            variants: self
                .variants
                .iter()
                .map(|variant| Variant {
                    signature: variant.signature.clone(),
                    factory: variant.factory.clone(),
                })
                .collect(),
            accepts: self.accepts.clone(),
        }
    }
}

impl<P: Providers> Entry<P> {
    fn single(descriptor: OpDescriptor, factory: OpFactory<P>) -> Self {
        let signature = Signature {
            input: descriptor.input.clone(),
            output: descriptor.output.clone(),
        };
        Self {
            descriptor,
            variants: vec![Variant { signature, factory }],
            accepts: None,
        }
    }

    /// The variant for values of type `produced`: the one registered for
    /// that very type, else one that takes any, else the first.
    fn variant(&self, produced: Option<&TypeTag>) -> Option<&Variant<P>> {
        let any = || {
            self.variants
                .iter()
                .find(|variant| variant.signature.input == TypeTag::Any)
        };
        match produced {
            Some(produced) => self
                .variants
                .iter()
                .find(|variant| variant.signature.input == *produced)
                .or_else(any),
            None => any(),
        }
        .or_else(|| self.variants.first())
    }

    /// Say in the descriptor what the variants have in common.
    fn summarize(&mut self) {
        let common = |pick: fn(&Signature) -> &TypeTag| {
            let mut tags = self.variants.iter().map(|variant| pick(&variant.signature));
            let first = tags.next().cloned().unwrap_or(TypeTag::Any);
            if tags.all(|tag| *tag == first) {
                first
            } else {
                TypeTag::Any
            }
        };
        let (input, output) = (
            common(|signature| &signature.input),
            common(|signature| &signature.output),
        );
        self.descriptor.signatures = if self.variants.len() > 1 {
            self.variants.iter().map(|variant| variant.signature.clone()).collect()
        } else {
            Vec::new()
        };
        self.descriptor.input = input;
        self.descriptor.output = output;
    }
}

/// The operators compiled into a worker, by reference.
pub struct Registry<P: Providers> {
    ops: BTreeMap<OpRef, Entry<P>>,
}

impl<P: Providers> Registry<P> {
    /// Say what the configuration of a registered operator is made of, so
    /// that tools can offer a form for it. Unknown references are ignored.
    pub fn with_params(&mut self, op: &str, params: impl IntoIterator<Item = ParamSpec>) -> &mut Self {
        if let Some(entry) = op.parse::<OpRef>().ok().and_then(|op| self.ops.get_mut(&op)) {
            entry.descriptor.params = params.into_iter().collect();
        }
        self
    }
}

impl<P: Providers> Clone for Registry<P> {
    fn clone(&self) -> Self { Self { ops: self.ops.clone() } }
}

impl<P: Providers> Default for Registry<P> {
    fn default() -> Self { Self::empty() }
}

/// An operator read from the configuration of a node, and checked.
fn configured<O: Operator>(config: &Value) -> Result<O, OpError> {
    let operator: O = parse_config(config).map_err(|error| match error {
        OpError::Config(reason) => OpError::Config(format!("{}: {reason}", O::OP)),
        other => other,
    })?;
    operator.check().map_err(|error| match error {
        OpError::Config(reason) => OpError::Config(format!("{}: {reason}", O::OP)),
        other => other,
    })?;
    Ok(operator)
}

/// An operator read from the configuration of a node, whose tenant has every
/// secret it names: a node that lacks one is refused before it starts.
fn with_its_secrets<P: Providers, O: Operator>(config: &Value, context: &OpContext<P>) -> Result<O, OpError> {
    let operator = configured::<O>(config)?;
    for secret in operator.secrets() {
        context.reveal(secret).map_err(|error| match error {
            OpError::Config(reason) => OpError::Config(format!("{}: {reason}", O::OP)),
            other => other,
        })?;
    }
    Ok(operator)
}

fn parse_config<C: DeserializeOwned>(config: &Value) -> Result<C, OpError> {
    let value = if config.is_null() {
        Value::Object(serde_json::Map::new())
    } else {
        config.clone()
    };
    serde_json::from_value(value).map_err(|e| OpError::Config(e.to_string()))
}

struct FnMap<In, Out, F>(F, std::marker::PhantomData<fn(&In) -> Out>);

impl<In, Out, F> DynMap for FnMap<In, Out, F>
where
    In: Any + Send + Sync,
    Out: Any + Send + Sync,
    F: Fn(&In, &mut Vec<Out>) + Send + Sync + 'static,
{
    fn apply(&self, input: &Datum, out: &mut Vec<Datum>) -> Result<(), OpError> {
        let mut typed = Vec::new();
        (self.0)(downcast::<In>(input)?, &mut typed);
        out.extend(typed.into_iter().map(datum));
        Ok(())
    }
}

struct FnKey<In, K, F>(F, std::marker::PhantomData<fn(&In) -> K>);

impl<In, K, F> DynKey for FnKey<In, K, F>
where
    In: Any + Send + Sync,
    K: Serialize + 'static,
    F: Fn(&In) -> K + Send + Sync + 'static,
{
    fn key(&self, input: &Datum) -> Result<Key, OpError> {
        Key::encode(&(self.0)(downcast::<In>(input)?)).map_err(OpError::Failed)
    }
}

impl<P: Providers> Registry<P> {
    /// A registry with no operator at all.
    #[must_use]
    pub fn empty() -> Self { Self { ops: BTreeMap::new() } }

    /// A registry holding the type-agnostic `std/*` operators
    /// (see [`crate::stdops`]).
    #[must_use]
    pub fn standard() -> Self {
        let mut registry = Self::empty();
        crate::stdops::register(&mut registry);
        registry
    }

    /// Register an operator from its descriptor and factory. Replaces any
    /// operator already registered under the same reference.
    pub fn register(&mut self, descriptor: OpDescriptor, factory: OpFactory<P>) -> &mut Self {
        self.ops
            .insert(descriptor.op.clone(), Entry::single(descriptor, factory));
        self
    }

    /// Register an operator for the type its descriptor says it takes, next
    /// to what the same reference is already registered for: an operator
    /// written for several types has one factory per type. Registering it
    /// again for a type replaces that one.
    pub fn register_variant(&mut self, descriptor: OpDescriptor, factory: OpFactory<P>) -> &mut Self {
        let same_kind = self
            .ops
            .get(&descriptor.op)
            .is_some_and(|entry| entry.descriptor.kind == descriptor.kind);
        if !same_kind {
            return self.register(descriptor, factory);
        }
        let Some(entry) = self.ops.get_mut(&descriptor.op) else {
            return self;
        };
        let signature = Signature {
            input: descriptor.input.clone(),
            output: descriptor.output.clone(),
        };
        match entry
            .variants
            .iter_mut()
            .find(|variant| variant.signature.input == signature.input)
        {
            Some(variant) => {
                variant.signature = signature;
                variant.factory = factory;
            }
            None => entry.variants.push(Variant { signature, factory }),
        }
        entry.descriptor.description = descriptor.description;
        entry.descriptor.params = descriptor.params;
        entry.summarize();
        self
    }

    fn describe_operator<O: Operator>(kind: OpKind, input: TypeTag, output: TypeTag) -> OpDescriptor {
        let mut descriptor = Self::describe(O::OP, kind, input, output, O::ABOUT);
        descriptor.params = O::params();
        descriptor
    }

    /// Register a [`Source`].
    ///
    /// # Panics
    ///
    /// Panics if `S::OP` is not a valid operator reference.
    pub fn source<S: Source>(&mut self) -> &mut Self {
        self.register(
            Self::describe_operator::<S>(OpKind::Source, TypeTag::Any, TypeTag::of::<S::Out>()),
            OpFactory::Source(Arc::new(|context: &OpContext<P>, config: &Value| {
                Ok(pipe::source_connector(
                    with_its_secrets::<P, S>(config, context)?,
                    context,
                ))
            })),
        );
        self.accepting::<S>()
    }

    /// Let the registry say whether a configuration of `O` can run without
    /// the context of a node.
    fn accepting<O: Operator>(&mut self) -> &mut Self {
        if let Ok(op) = OpRef::new(O::OP)
            && let Some(entry) = self.ops.get_mut(&op)
        {
            entry.accepts = Some(Arc::new(|config: &Value| configured::<O>(config).map(drop)));
        }
        self
    }

    /// Whether the node of a graph may run `op` with `config`, fed with
    /// values of type `produced` (see [`Registry::get_for`]): what the
    /// operator checks of its configuration, and nothing that needs the
    /// outside world. An operator that is not registered accepts nothing.
    ///
    /// # Errors
    ///
    /// Why the configuration is refused, in words.
    pub fn accepts(&self, op: &OpRef, produced: Option<&TypeTag>, config: &Value) -> Result<(), OpError> {
        let unknown = || OpError::Config(format!("`{op}` is not a registered operator"));
        let entry = self.ops.get(op).ok_or_else(unknown)?;
        match &entry.variant(produced).ok_or_else(unknown)?.factory {
            OpFactory::Map(factory) => factory(config).map(drop),
            OpFactory::Key(factory) => factory(config).map(drop),
            OpFactory::Window(factory) => factory(config).map(drop),
            OpFactory::Aggregate(factory) => factory(config).map(drop),
            OpFactory::Source(_) | OpFactory::Sink(_) => {
                entry.accepts.as_ref().map_or(Ok(()), |accepts| accepts(config))
            }
        }
    }

    /// Register a [`Sink`] for values of type `In`. A sink written for any
    /// type is registered once per type it is given.
    ///
    /// # Panics
    ///
    /// Panics if `S::OP` is not a valid operator reference.
    pub fn sink<In: Any + Send + Sync, S: Sink<In>>(&mut self) -> &mut Self {
        self.register_variant(
            Self::describe_operator::<S>(OpKind::Sink, pipe::tag::<In>(), TypeTag::Any),
            OpFactory::Sink(Arc::new(|context: &OpContext<P>, config: &Value| {
                Ok(pipe::sink_connector::<P, In, S>(
                    with_its_secrets::<P, S>(config, context)?,
                    context,
                ))
            })),
        );
        self.accepting::<S>()
    }

    /// Register a [`Transformer`] for what flows in as `In`. `example` is any
    /// value of it: its `transform` is called once to learn what the
    /// transformer is (a map, a `key_by`, a window or an aggregate) and the
    /// types it takes and gives.
    ///
    /// # Errors
    ///
    /// The transformer does several things at once.
    ///
    /// # Panics
    ///
    /// Panics if `T::OP` is not a valid operator reference.
    pub fn transformer<In: 'static, T: Transformer<In>>(&mut self, example: &T) -> Result<&mut Self, OpError> {
        let (built, input, output) = pipe::build::<In, T>(example)?;
        let kind = built.kind();
        let other = move || {
            OpError::Config(format!(
                "what `{}` is depends on its configuration: it was registered as a {kind}",
                T::OP
            ))
        };
        let factory = match built {
            Built::Map(_) => OpFactory::Map(Arc::new(move |config: &Value| {
                match pipe::build::<In, T>(&configured::<T>(config)?)?.0 {
                    Built::Map(map) => Ok(map),
                    _ => Err(other()),
                }
            })),
            Built::Key(_) => OpFactory::Key(Arc::new(move |config: &Value| {
                match pipe::build::<In, T>(&configured::<T>(config)?)?.0 {
                    Built::Key(key) => Ok(key),
                    _ => Err(other()),
                }
            })),
            Built::Window(_) => OpFactory::Window(Arc::new(move |config: &Value| {
                match pipe::build::<In, T>(&configured::<T>(config)?)?.0 {
                    Built::Window(window) => Ok(window),
                    _ => Err(other()),
                }
            })),
            Built::Aggregate(_) => OpFactory::Aggregate(Arc::new(move |config: &Value| {
                match pipe::build::<In, T>(&configured::<T>(config)?)?.0 {
                    Built::Aggregate(aggregate) => Ok(aggregate),
                    _ => Err(other()),
                }
            })),
        };
        Ok(self.register_variant(Self::describe_operator::<T>(kind, input, output), factory))
    }

    fn describe(op: &str, kind: OpKind, input: TypeTag, output: TypeTag, about: &str) -> OpDescriptor {
        OpDescriptor {
            op: OpRef::new(op).unwrap_or_else(|e| panic!("{e}")),
            kind,
            input,
            output,
            description: about.to_owned(),
            params: Vec::new(),
            signatures: Vec::new(),
        }
    }

    /// Register a source connector producing values of type `Out`.
    ///
    /// # Panics
    ///
    /// Panics if `op` is not a valid operator reference.
    pub fn register_source<Out: 'static>(
        &mut self,
        op: &str,
        about: &str,
        factory: impl Fn(&OpContext<P>, &Value) -> Result<Arc<dyn SourceConnector>, OpError> + Send + Sync + 'static,
    ) -> &mut Self {
        self.register(
            Self::describe(op, OpKind::Source, TypeTag::Any, TypeTag::of::<Out>(), about),
            OpFactory::Source(Arc::new(factory)),
        )
    }

    /// Register a sink connector. `input` is the type it accepts
    /// ([`TypeTag::Any`] for a type-agnostic sink).
    ///
    /// # Panics
    ///
    /// Panics if `op` is not a valid operator reference.
    pub fn register_sink(
        &mut self,
        op: &str,
        about: &str,
        input: TypeTag,
        factory: impl Fn(&OpContext<P>, &Value) -> Result<Arc<dyn SinkConnector>, OpError> + Send + Sync + 'static,
    ) -> &mut Self {
        self.register(
            Self::describe(op, OpKind::Sink, input, TypeTag::Any, about),
            OpFactory::Sink(Arc::new(factory)),
        )
    }

    /// Register a `map`/`filter`: one input, at most one output.
    ///
    /// # Panics
    ///
    /// Panics if `op` is not a valid operator reference.
    pub fn register_map<In, Out>(
        &mut self,
        op: &str,
        about: &str,
        f: impl Fn(&In) -> Option<Out> + Send + Sync + 'static,
    ) -> &mut Self
    where
        In: Any + Send + Sync,
        Out: Any + Send + Sync,
    {
        let f = Arc::new(f);
        self.register_map_with(op, about, move |_: serde::de::IgnoredAny| {
            let f = f.clone();
            move |input: &In, out: &mut Vec<Out>| out.extend(f(input))
        })
    }

    /// Register a `flat_map`: one input, any number of outputs.
    ///
    /// # Panics
    ///
    /// Panics if `op` is not a valid operator reference.
    pub fn register_flat_map<In, Out>(
        &mut self,
        op: &str,
        about: &str,
        f: impl Fn(&In, &mut Vec<Out>) + Send + Sync + 'static,
    ) -> &mut Self
    where
        In: Any + Send + Sync,
        Out: Any + Send + Sync,
    {
        let f = Arc::new(f);
        self.register_map_with(op, about, move |_: serde::de::IgnoredAny| {
            let f = f.clone();
            move |input: &In, out: &mut Vec<Out>| f(input, out)
        })
    }

    /// Register a configurable transformation. `build` receives the node
    /// configuration, deserialized into `C`, once per actor and returns the
    /// function applied to every record.
    ///
    /// # Panics
    ///
    /// Panics if `op` is not a valid operator reference.
    pub fn register_map_with<C, In, Out, F>(
        &mut self,
        op: &str,
        about: &str,
        build: impl Fn(C) -> F + Send + Sync + 'static,
    ) -> &mut Self
    where
        C: DeserializeOwned,
        In: Any + Send + Sync,
        Out: Any + Send + Sync,
        F: Fn(&In, &mut Vec<Out>) + Send + Sync + 'static,
    {
        self.register(
            Self::describe(op, OpKind::Map, TypeTag::of::<In>(), TypeTag::of::<Out>(), about),
            OpFactory::Map(Arc::new(move |config: &Value| {
                let f = build(parse_config::<C>(config)?);
                Ok(Arc::new(FnMap(f, std::marker::PhantomData)) as Arc<dyn DynMap>)
            })),
        )
    }

    /// Register a key extractor.
    ///
    /// # Panics
    ///
    /// Panics if `op` is not a valid operator reference.
    pub fn register_key<In, K>(
        &mut self,
        op: &str,
        about: &str,
        f: impl Fn(&In) -> K + Send + Sync + 'static,
    ) -> &mut Self
    where
        In: Any + Send + Sync,
        K: Serialize + 'static,
    {
        let f = Arc::new(f);
        self.register(
            Self::describe(op, OpKind::KeyBy, TypeTag::of::<In>(), TypeTag::Any, about),
            OpFactory::Key(Arc::new(move |_config: &Value| {
                let f = f.clone();
                Ok(Arc::new(FnKey(move |input: &In| f(input), std::marker::PhantomData)) as Arc<dyn DynKey>)
            })),
        )
    }

    /// Register a window assigner built from the node configuration.
    ///
    /// # Panics
    ///
    /// Panics if `op` is not a valid operator reference.
    pub fn register_window<C, W>(
        &mut self,
        op: &str,
        about: &str,
        build: impl Fn(C) -> Result<W, OpError> + Send + Sync + 'static,
    ) -> &mut Self
    where
        C: DeserializeOwned,
        W: WindowAssigner,
    {
        self.register(
            Self::describe(op, OpKind::Window, TypeTag::Any, TypeTag::Any, about),
            OpFactory::Window(Arc::new(move |config: &Value| {
                Ok(Arc::new(build(parse_config::<C>(config)?)?) as Arc<dyn WindowAssigner>)
            })),
        )
    }

    /// Register a typed aggregator built from the node configuration.
    ///
    /// # Panics
    ///
    /// Panics if `op` is not a valid operator reference.
    pub fn register_aggregator<C, A>(
        &mut self,
        op: &str,
        about: &str,
        build: impl Fn(C) -> Result<A, OpError> + Send + Sync + 'static,
    ) -> &mut Self
    where
        C: DeserializeOwned,
        A: Aggregator,
    {
        self.register(
            Self::describe(
                op,
                OpKind::Aggregate,
                TypeTag::of::<A::In>(),
                TypeTag::of::<crate::data::Windowed<A::Out>>(),
                about,
            ),
            OpFactory::Aggregate(Arc::new(move |config: &Value| {
                Ok(Arc::new(TypedAggregator(build(parse_config::<C>(config)?)?)) as Arc<dyn DynAggregator>)
            })),
        )
    }

    /// Register an already-erased aggregator (for type-agnostic ones).
    ///
    /// # Panics
    ///
    /// Panics if `op` is not a valid operator reference.
    pub fn register_dyn_aggregator(
        &mut self,
        op: &str,
        about: &str,
        input: TypeTag,
        output: TypeTag,
        factory: impl Fn(&Value) -> Result<Arc<dyn DynAggregator>, OpError> + Send + Sync + 'static,
    ) -> &mut Self {
        self.register(
            Self::describe(op, OpKind::Aggregate, input, output, about),
            OpFactory::Aggregate(Arc::new(factory)),
        )
    }

    /// Merge another registry into this one. Operators of `other` win.
    #[must_use]
    pub fn with(mut self, other: Registry<P>) -> Self {
        self.ops.extend(other.ops);
        self
    }

    /// Look an operator up. For one registered for several types, the way
    /// of building it that takes any type when there is one, else the first:
    /// see [`Registry::get_for`].
    #[must_use]
    pub fn get(&self, op: &OpRef) -> Option<(&OpDescriptor, &OpFactory<P>)> { self.get_for(op, None) }

    /// Look an operator up, as built for values of type `produced` (what the
    /// node feeding it produces, `None` when it has no input or its type is
    /// not known).
    #[must_use]
    pub fn get_for(&self, op: &OpRef, produced: Option<&TypeTag>) -> Option<(&OpDescriptor, &OpFactory<P>)> {
        let entry = self.ops.get(op)?;
        let variant = entry.variant(produced)?;
        Some((&entry.descriptor, &variant.factory))
    }

    /// The descriptors of every registered operator.
    #[must_use]
    pub fn catalog(&self) -> Catalog {
        Catalog {
            ops: self
                .ops
                .iter()
                .map(|(op, entry)| (op.clone(), entry.descriptor.clone()))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use moonpool_core::TokioProviders;

    #[test]
    fn operators_build_without_configuration() {
        let mut registry = Registry::<TokioProviders>::standard();
        registry.register_map("test/len@1", "", |text: &String| Some(text.len()));
        registry.register_flat_map("test/chars@1", "", |text: &String, out: &mut Vec<char>| {
            out.extend(text.chars());
        });
        registry.register_key("test/key@1", "", |text: &String| text.clone());
        for config in [Value::Null, serde_json::json!({}), serde_json::json!({"ignored": 1})] {
            for op in ["test/len@1", "test/chars@1", "std/identity@1"] {
                let Some((_, OpFactory::Map(factory))) = registry.get(&OpRef::new(op).unwrap()) else {
                    panic!("{op} is not a map");
                };
                let map = factory(&config).unwrap_or_else(|e| panic!("{op} with {config}: {e}"));
                let mut out = Vec::new();
                map.apply(&datum("abc".to_owned()), &mut out).unwrap();
                assert!(!out.is_empty());
            }
            let Some((_, OpFactory::Key(factory))) = registry.get(&OpRef::new("test/key@1").unwrap()) else {
                panic!("not a key");
            };
            let key = factory(&config).unwrap().key(&datum("abc".to_owned())).unwrap();
            assert_eq!(key.decode::<String>().unwrap(), "abc");
            let Some((_, OpFactory::Aggregate(factory))) = registry.get(&OpRef::new("std/count@1").unwrap()) else {
                panic!("not an aggregate");
            };
            factory(&config).unwrap();
        }
        // A window needs its size: a missing configuration is a clear error.
        let Some((_, OpFactory::Window(factory))) = registry.get(&OpRef::new("std/tumbling@1").unwrap()) else {
            panic!("not a window");
        };
        assert!(matches!(factory(&Value::Null), Err(OpError::Config(_))));
        assert!(factory(&serde_json::json!({"size": "5m"})).is_ok());
    }

    #[test]
    fn secrets_resolve_per_tenant() {
        let tenant = TenantId::new("acme").unwrap();
        let context = OpContext {
            providers: TokioProviders::new(),
            clock: Arc::new(crate::clock::SystemClock),
            secrets: Arc::new(crate::clock::StaticSecrets::new().with(&tenant, "token", "s3cr3t")),
            kv: crate::kv::MemKv::shared(),
            shared: Arc::default(),
            tenant,
            graph: GraphId::new("g").unwrap(),
            node: NodeId::new("n").unwrap(),
        };
        assert_eq!(context.secret_or_literal(&serde_json::json!("plain")).unwrap(), "plain");
        assert_eq!(
            context
                .secret_or_literal(&serde_json::json!({"secret": "token"}))
                .unwrap(),
            "s3cr3t"
        );
        assert!(
            context
                .secret_or_literal(&serde_json::json!({"secret": "other"}))
                .is_err()
        );
        assert!(context.secret_or_literal(&serde_json::json!(42)).is_err());
    }

    /// Sums `u64`s; its accumulator is a `(sum, count)` pair.
    struct Sum;

    impl crate::ops::Aggregator for Sum {
        type In = u64;
        type Acc = (u64, u32);
        type Out = u64;
        fn init(&self) -> Self::Acc { (0, 0) }
        fn add(&self, acc: &mut Self::Acc, item: &u64) {
            acc.0 += item;
            acc.1 += 1;
        }
        fn merge(&self, into: &mut Self::Acc, other: &Self::Acc) {
            into.0 += other.0;
            into.1 += other.1;
        }
        fn finish(&self, acc: &Self::Acc) -> u64 { acc.0 }
    }

    #[test]
    fn typed_aggregators_add_merge_and_round_trip() {
        let mut registry = Registry::<TokioProviders>::empty();
        let returned: *const Registry<TokioProviders> =
            registry.register_aggregator("test/sum@1", "Sums.", |_: Value| Ok(Sum));
        assert!(std::ptr::eq(returned, &raw const registry));
        let op = OpRef::new("test/sum@1").unwrap();
        let descriptor = registry.catalog().get(&op).unwrap().clone();
        assert_eq!(descriptor.kind, OpKind::Aggregate);
        assert_eq!(descriptor.input, TypeTag::of::<u64>());
        let Some((_, OpFactory::Aggregate(factory))) = registry.get(&op) else {
            panic!("not an aggregate");
        };
        let sum = factory(&Value::Null).unwrap();
        let mut a = sum.init();
        let mut b = sum.init();
        sum.add(&mut a, &datum(3_u64)).unwrap();
        sum.add(&mut a, &datum(4_u64)).unwrap();
        sum.add(&mut b, &datum(10_u64)).unwrap();
        assert!(sum.add(&mut b, &datum("not a u64")).is_err());
        sum.merge(&mut a, &b).unwrap();
        let bytes = sum.encode(&a).unwrap();
        assert_eq!(bytes, postcard::to_allocvec(&(17_u64, 3_u32)).unwrap());
        let decoded = sum.decode(&bytes).unwrap();
        let key = crate::data::Key::encode(&"k".to_owned()).unwrap();
        let window = crate::time::Window::new(0, 10);
        let out = sum.finish(&decoded, window, &key).unwrap();
        let out = out.downcast_ref::<crate::data::Windowed<u64>>().unwrap();
        assert_eq!((out.value, out.window, &out.key), (17, window, &key));
    }

    #[test]
    fn registries_clone_merge_and_describe_parameters() {
        let mut base = Registry::<TokioProviders>::empty();
        base.register_map("test/a@1", "first", |x: &u8| Some(*x));
        let mut other = Registry::<TokioProviders>::empty();
        other.register_map("test/a@1", "replaced", |x: &u8| Some(*x));
        other.register_map("test/b@1", "second", |x: &u8| Some(*x));
        let copy = base.clone();
        assert_eq!(copy.catalog(), base.catalog());
        let mut merged = base.with(other);
        let catalog = merged.catalog();
        assert_eq!(catalog.ops.len(), 2);
        let a = OpRef::new("test/a@1").unwrap();
        assert_eq!(catalog.get(&a).unwrap().description, "replaced");

        let returned: *const Registry<TokioProviders> = merged
            .with_params("test/b@1", [ParamSpec::new("size", "duration", "How long.")])
            .with_params("nope/x@1", [ParamSpec::new("ignored", "text", "")]);
        assert!(std::ptr::eq(returned, &raw const merged));
        let b = OpRef::new("test/b@1").unwrap();
        assert_eq!(merged.catalog().get(&b).unwrap().params[0].name, "size");

        let mut catalog = Catalog::default();
        catalog.insert(merged.catalog().get(&b).unwrap().clone());
        assert_eq!(catalog.ops.keys().collect::<Vec<_>>(), [&b]);
    }

    #[test]
    fn kinds_and_type_tags_print_and_parse() {
        let kinds = [
            (OpKind::Source, "source"),
            (OpKind::Map, "map"),
            (OpKind::KeyBy, "key_by"),
            (OpKind::Window, "window"),
            (OpKind::Aggregate, "aggregate"),
            (OpKind::Sink, "sink"),
        ];
        for (kind, text) in kinds {
            assert_eq!(kind.to_string(), text);
        }
        assert_eq!(TypeTag::Any.to_string(), "*");
        assert_eq!(TypeTag::Named("a::B".into()).to_string(), "a::B");
        for tag in [TypeTag::Any, TypeTag::Named("a::B".into())] {
            let json = serde_json::to_string(&tag).unwrap();
            assert_eq!(serde_json::from_str::<TypeTag>(&json).unwrap(), tag);
        }
    }
}
