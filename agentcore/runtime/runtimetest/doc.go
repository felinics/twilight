package runtimetest

// The suite is Store-parameterized like session/sessiontest and
// run/sessionstore/storetest: the reference memory store and every durable adapter run
// the same assertions. Assertions on the projection of Run facts into
// conversation entries (TRN-MAP) live in the RUN-CMP-2 suite, which observes
// them through the Runtime's group composition and the chatlog projections;
// this suite covers the Coordinator, the surface projection and the recovery
// table.
