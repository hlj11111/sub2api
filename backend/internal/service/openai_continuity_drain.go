package service

import (
	"context"
	"time"
)

type continuityUpstreamLifetimeKey struct{}

// Keep enough grace to drain terminal usage after a disconnect, but do not let
// an orphaned upstream request renew the session lease indefinitely. Never free
// the lease before Forward exits; a new turn must not overlap the old operation.
func (s *OpenAIGatewayService) withContinuityDrainDeadline(ctx context.Context) (context.Context, func()) {
	if !continuityEnabled(ctx) {
		return ctx, func() {}
	}
	grace := 20 * time.Second
	if s.cfg != nil && s.cfg.Gateway.SessionRecoveryDisconnectGraceSeconds > 0 {
		grace = time.Duration(s.cfg.Gateway.SessionRecoveryDisconnectGraceSeconds) * time.Second
	}
	return withContinuityDrainGrace(ctx, grace)
}

func withContinuityDrainGrace(ctx context.Context, grace time.Duration) (context.Context, func()) {
	upstream, cancel := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
			return
		case <-ctx.Done():
		}
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			cancel()
		}
	}()
	return context.WithValue(ctx, continuityUpstreamLifetimeKey{}, upstream), func() { close(done); cancel() }
}

func detachContinuityUpstreamContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if upstream, ok := ctx.Value(continuityUpstreamLifetimeKey{}).(context.Context); ok {
		return upstream, func() {}
	}
	return detachUpstreamContext(ctx)
}
