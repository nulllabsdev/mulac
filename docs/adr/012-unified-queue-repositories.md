# ADR-012: Unified queue repositories

**Status:** Accepted; supersedes ADR-007 and amends ADR-008; execution bridges amended by ADR-013 (commanding) and ADR-014 (other queues)

## Context

Commanding uses one PostgreSQL adapter for the storage ports of its queue.
Use-case-specific forwarding repositories add construction and type indirection
without owning execution behavior. Eventing, inbox, and outbox are being migrated
in stages to the same boundary.

## Decision

Each queue has one Diesel PostgreSQL repository implementing its store, reserve,
process, and sweep ports. Consumers and synchronous HTTP execution bridges are
generic over their repository and share it through `Arc<R>`. Synchronous code
accepts borrowed repository implementations; it does not require `'static`.
Recorders and sweepers depend directly on their respective trait-object ports.
Dispatcher, gateway, transport, and publisher dependencies remain separate.
Async HTTP bridges erase repository types and require `'static` only at the
background-task boundary. Public request/response contracts remain unchanged.

## Consequences

Storage rules remain queue-specific; there is no universal queue repository.
Redundant forwarding wrappers are removed without compatibility aliases, requiring
caller migration. The generic consumer interface is an intentional exception to
ADR-008's trait-object preference. This decision does not alter retry semantics,
start recovery workers, or introduce atomic handler/event persistence.

## References

- `queue-commanding-parity-implementation-plan.md`
- ADR-007: use-case-split repositories
- ADR-008: shared port dependencies
