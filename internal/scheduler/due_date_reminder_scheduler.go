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
// upcoming due dates into reminder notifications.
type DueDateReminderSchedulerConfig struct {
	// TickInterval is how often the sweep runs; default 1 hour.
	TickInterval time.Duration
	// LeadTime is how far ahead of the due date a reminder is sent; default
	// 24 hours. Due dates are calendar dates, so the bound is date-aligned.
	LeadTime time.Duration
	// BatchSize caps how many due items one sweep handles; default 500.
	BatchSize int
}

// DefaultDueDateReminderSchedulerConfig returns the built-in defaults.
func DefaultDueDateReminderSchedulerConfig() DueDateReminderSchedulerConfig {
	return DueDateReminderSchedulerConfig{
		TickInterval: time.Hour,
		LeadTime:     24 * time.Hour,
		BatchSize:    500,
	}
}

// DueDateReminderScheduler periodically creates a "reminder" notification for
// the assignee of every open item whose due date is approaching. Delivery is
// idempotent: the notification's delivery key is derived from the item, due
// date, and assignee, so the unique index drops repeats and a changed due date
// re-reminds. Email is handled by the existing notification email batching.
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
	if config.LeadTime <= 0 {
		config.LeadTime = defaults.LeadTime
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
		slog.Duration("lead_time", s.config.LeadTime),
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
	// the process was down; the delivery key makes the extra pass idempotent.
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

// RunOnce scans for due-soon items and creates their reminder notifications.
// Exported so tests can drive a deterministic pass with an injected clock.
func (s *DueDateReminderScheduler) RunOnce(ctx context.Context, now time.Time) {
	if s.notifications == nil {
		return
	}
	from := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	to := from.Add(s.config.LeadTime)

	items, err := s.items.ListDueSoonOpenItems(from, to, s.config.BatchSize)
	if err != nil {
		slog.Warn("due-date reminders: failed to list due items",
			slog.String("component", "reminders"), slog.Any("error", err))
		return
	}
	for i := range items {
		if err := ctx.Err(); err != nil {
			return
		}
		if err := s.notify(items[i], now); err != nil {
			slog.Warn("due-date reminders: failed to create reminder",
				slog.String("component", "reminders"),
				slog.Int("item_id", items[i].ItemID),
				slog.Any("error", err))
		}
	}
}

func (s *DueDateReminderScheduler) notify(item repository.DueSoonItem, now time.Time) error {
	itemKey := fmt.Sprintf("%s-%d", item.WorkspaceKey, item.ItemNumber)
	dueDate := item.DueDate.Format("2006-01-02")
	claimed, err := s.items.ClaimDueDateReminder(item.ItemID, item.AssigneeID, dueDate)
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}
	workspaceID, itemID := item.WorkspaceID, item.ItemID
	_, err = s.notifications.CreateNotification(models.Notification{
		UserID:             item.AssigneeID,
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
		DeliveryKey:        fmt.Sprintf("due-reminder:%d:%s:%d", itemID, dueDate, item.AssigneeID),
	})
	return err
}
