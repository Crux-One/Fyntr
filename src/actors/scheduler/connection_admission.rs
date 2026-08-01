use std::collections::HashSet;

use log::{debug, warn};

use crate::{
    actors::connection_limit::{ConnectionLimiter, RegisterError},
    flow::FlowId,
    limits::MaxConnections,
};

pub(super) struct ConnectionAdmission {
    limiter: ConnectionLimiter,
    pending_connection_task_ids: HashSet<FlowId>,
}

impl ConnectionAdmission {
    pub(super) fn new() -> Self {
        Self {
            limiter: ConnectionLimiter::new(),
            pending_connection_task_ids: HashSet::new(),
        }
    }

    pub(super) fn set_max_connections(&mut self, max_connections: MaxConnections) {
        self.limiter.set_max_connections(max_connections);
    }

    pub(super) fn max_connections(&self) -> MaxConnections {
        self.limiter.max_connections()
    }

    pub(super) fn current_connection_count(&self) -> usize {
        self.limiter.current()
    }

    pub(super) fn pending_connection_tasks(&self) -> usize {
        self.pending_connection_task_ids.len()
    }

    pub(super) fn try_reserve_connection_task(
        &mut self,
        flow_id: FlowId,
    ) -> Result<(), RegisterError> {
        if let Some(limit) = self.max_connections() {
            let in_flight = self
                .current_connection_count()
                .saturating_add(self.pending_connection_tasks());
            if in_flight >= limit.get() {
                return Err(RegisterError::MaxConnectionsReached { max: limit.get() });
            }
        }

        if !self.pending_connection_task_ids.insert(flow_id) {
            warn!(
                "flow{}: duplicate pending connection task reservation",
                flow_id.0
            );
            return Err(RegisterError::DuplicateConnectionTask { flow_id });
        }

        self.log_pending_connection_task_diagnostics("reserved");
        Ok(())
    }

    pub(super) fn release_connection_task_reservation(&mut self, flow_id: FlowId) -> bool {
        if !self.pending_connection_task_ids.remove(&flow_id) {
            warn!(
                "flow{}: connection task reservation was not pending when released",
                flow_id.0
            );
            return false;
        }

        self.log_pending_connection_task_diagnostics("released");
        true
    }

    pub(super) fn try_acquire_registered_connection(&self) -> Result<(), RegisterError> {
        self.limiter.try_acquire()
    }

    pub(super) fn release_registered_connection(&self) {
        self.limiter.release();
    }

    pub(super) fn has_registered_capacity(&self) -> bool {
        match self.max_connections() {
            Some(limit) => self.current_connection_count() < limit.get(),
            None => true,
        }
    }

    fn log_pending_connection_task_diagnostics(&self, action: &str) {
        let pending = self.pending_connection_tasks();
        if !should_log_pending_connection_task_diagnostics(pending) {
            return;
        }

        let max_connections = self.max_connections();
        match max_connections {
            Some(limit) => debug!(
                "connection task {} (pending_connect_tasks: {}, connections: {}/{})",
                action,
                pending,
                self.current_connection_count(),
                limit
            ),
            None => debug!(
                "connection task {} (pending_connect_tasks: {}, connections: {})",
                action,
                pending,
                self.current_connection_count()
            ),
        }
    }
}

fn should_log_pending_connection_task_diagnostics(pending: usize) -> bool {
    pending > 16 && (pending.is_power_of_two() || pending.is_multiple_of(100))
}
