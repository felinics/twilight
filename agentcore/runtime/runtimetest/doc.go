package runtimetest

// The suite is Store-parameterized like session/sessiontest and
// turn/sessionstore/storetest: the reference memory store and every durable adapter execute
// the same assertions. Assertions on the projection of Turn facts into
// conversation entries live in the suite, which observes
// them through the Runtime's group composition and the chatlog projections;
// this suite covers the Coordinator, the surface projection and the recovery
// table.
