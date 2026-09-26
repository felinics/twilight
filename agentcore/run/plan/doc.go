// Package plan derives what a Run's MachineState asks for next and persists
// none of it. Next is the at-most-one pending action of a Run (RUN-MCH-4),
// with the read-only queries WaitingCalls, ExecutingCalls and NeedsRecovery
// the application inspects when Next has no executable action; the Loop
// re-derives the action after every Load. RecoveryTargets, RecoveryCommand
// and RecoveryCommands derive the takeover dispositions of the Executing
// targets (RUN-CMT-7): the recovery command of each target with its derived
// identity. agent/run/reconcile decides which dispositions a new owner
// actually issues.
package plan
