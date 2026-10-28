package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"windshift/internal/database"
	"windshift/internal/models"
	"windshift/internal/repository"
	"windshift/internal/services"
)

// DueDateReminderSchedulerConfig configures the background sweep that turns
// upcoming due dates into reminder notifications for users who opted in.
type DueDateReminderSchedulerConfig struct {
	// TickInterval is how often the sweep runs; default 1 hour.
	TickInterval time.Duration
	// BatchSize caps how many due items one sweep handles; default 500.
	BatchSize int
}

// DefaultDueDateReminderSchedulerConfig returns the built-in defaults.
func DefaultDueDateReminderSchedulerConfig() DueDateReminderSchedulerConfig {
	return DueDateReminderSchedulerConfig{
		TickInterval: time.Hour,
		BatchSize:    500,
	}
}

// DueDateReminderScheduler periodically creates a "reminder" notification for
// opted-in users about their open items whose due date is approaching. The
// reminder is never automatic: only users who enabled due-date reminders in
// their profile are notified, using their configured lead time. Assignees are
// reminded for regular items; personal-workspace items without an assignee
// remind their owner. Delivery is idempotent via a per-(item, user, due date)
// ledger and a notification delivery key. Email rides the existing
// notification email batching.
type DueDateReminderScheduler struct {
	db            database.Database
	items         *repository.ItemRepository
	notifications *services.NotificationService
	config        DueDateReminderSchedulerConfig
	stopChan      chan struct{}
	wg            sync.WaitGroup
	// now is overridable for tests; production uses wall-clock time.
	now func() time.Time
}

// NewDueDateReminderScheduler constructs the scheduler. Call Start() to begin.
func NewDueDateReminderScheduler(db database.Database, notifications *services.NotificationService, config DueDateReminderSchedulerConfig) *DueDateReminderScheduler {
	defaults := DefaultDueDateReminderSchedulerConfig()
	if config.TickInterval <= 0 {
		config.TickInterval = defaults.TickInterval
	}
	if config.BatchSize <= 0 {
		config.BatchSize = defaults.BatchSize
	}
	return &DueDateReminderScheduler{
		db:            db,
		items:         repository.NewItemRepository(db),
		notifications: notifications,
		config:        config,
		stopChan:      make(chan struct{}),
		now:           time.Now,
	}
}

// Start launches the background sweep. Idempotent per scheduler instance.
func (s *DueDateReminderScheduler) Start() {
	s.wg.Add(1)
	go s.run()
	slog.Info("due-date reminder scheduler started",
		slog.String("component", "reminders"),
		slog.Duration("tick_interval", s.config.TickInterval),
	)
}

// Stop signals shutdown and waits for the worker to drain.
func (s *DueDateReminderScheduler) Stop() {
	close(s.stopChan)
	s.wg.Wait()
}

func (s *DueDateReminderScheduler) run() {
	defer s.wg.Done()
	// Sweep once at startup so a restart catches reminders that came due while
	// the process was down; the ledger makes the extra pass idempotent.
	s.RunOnce(context.Background(), s.now())

	t := time.NewTicker(s.config.TickInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stopChan:
			return
		case <-t.C:
			s.RunOnce(context.Background(), s.now())
		}
	}
}

// RunOnce scans for opted-in recipients' due-soon items and creates their
// reminder notifications. Exported so tests can drive a deterministic pass
// with an injected clock.
func (s *DueDateReminderScheduler) RunOnce(ctx context.Context, now time.Time) {
	if s.notifications == nil {
		return
	}
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	// Query every opted-in recipient's items inside the widest allowed window,
	// then apply each recipient's own lead time in Go so the window never needs
	// engine-specific date arithmetic.
	horizon := today.AddDate(0, 0, services.MaxReminderDays)

	items, err := s.items.ListDueSoonItemsForReminderUsers(today, horizon, s.config.BatchSize)
	if err != nil {
		slog.Warn("due-date reminders: failed to list due items",
			slog.String("component", "reminders"), slog.Any("error", err))
		return
	}
	for i := range items {
		if err := ctx.Err(); err != nil {
			return
		}
		item := items[i]
		limit := today.AddDate(0, 0, item.ReminderDays)
		if item.DueDate.After(limit) {
			continue
		}
		if err := s.notify(item, now); err != nil {
			slog.Warn("due-date reminders: failed to create reminder",
				slog.String("component", "reminders"),
				slog.Int("item_id", item.ItemID),
				slog.Any("error", err))
		}
	}
}

func (s *DueDateReminderScheduler) notify(item repository.DueSoonItem, now time.Time) error {
	itemKey := fmt.Sprintf("%s-%d", item.WorkspaceKey, item.ItemNumber)
	dueDate := item.DueDate.Format("2006-01-02")
	claimed, err := s.items.ClaimDueDateReminder(item.ItemID, item.RecipientID, dueDate)
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}
	workspaceID, itemID := item.WorkspaceID, item.ItemID
	_, err = s.notifications.CreateNotification(models.Notification{
		UserID:             item.RecipientID,
		Title:              fmt.Sprintf("Reminder: %s is due soon", itemKey),
		Message:            fmt.Sprintf("%s is due on %s", item.Title, dueDate),
		Type:               "reminder",
		Timestamp:          now,
		Read:               false,
		ActionURL:          fmt.Sprintf("/workspaces/%d/items/%d", workspaceID, itemID),
		AuthorizationScope: models.NotificationScopeWorkspace,
		WorkspaceID:        &workspaceID,
		ItemID:             &itemID,
		SourceType:         "item.due_reminder",
		SourceID:           &itemID,
		DeliveryKey:        fmt.Sprintf("due-reminder:%d:%s:%d", itemID, dueDate, item.RecipientID),
	})
	return err
}
